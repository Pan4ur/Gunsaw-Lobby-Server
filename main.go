package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	protocolVersion  = "2"
	legacyModVersion = "0.4.0"
	lobbyTTL         = 45 * time.Second
	pendingPeerTTL   = 15 * time.Second
	peerTTL          = 30 * time.Second
	defaultBanTTL    = time.Hour
	maxBanTTL        = 24 * time.Hour
	maxDatagram      = 1400

	udpAuth       = byte(1)
	udpAuthOK     = byte(2)
	udpData       = byte(3)
	udpForwarded  = byte(4)
	udpAuthFailed = byte(5)
	udpP2PEnable  = byte(6)
	udpCandidate  = byte(7)
	udpDirectData = byte(8)
	udpKeepAlive  = byte(9)
	p2pKeySize    = 16
)

var udpMagic = []byte{'G', 'U', 'D', 'P'}

type lobby struct {
	ID                  string       `json:"id"`
	Name                string       `json:"name"`
	HostName            string       `json:"hostName"`
	Map                 string       `json:"map"`
	MaxPlayers          int          `json:"maxPlayers"`
	Players             int          `json:"players"`
	PVP                 bool         `json:"pvp"`
	CanGrab             bool         `json:"canGrab"`
	GrabOnlyUnconscious bool         `json:"grabOnlyUnconscious"`
	AllowRespawn        bool         `json:"allowRespawn"`
	RespawnTime         int          `json:"respawnTime"`
	RespawnAtStart      bool         `json:"respawnAtStart"`
	ConnectionMode      string       `json:"connectionMode"`
	HostPort            int          `json:"hostPort"`
	HostIP              string       `json:"hostIp"`
	ModVersion          string       `json:"modVersion"`
	UpdatedAt           time.Time    `json:"-"`
	HostKey             string       `json:"-"`
	HostPeer            uint16       `json:"-"`
	HostAddr            *net.UDPAddr `json:"-"`
	peers               map[string]peer
	bannedIPs           map[string]time.Time
	usedPeerIDs         map[uint16]bool
	HostP2P             bool
	P2PKey              []byte
}

type peer struct {
	key           string
	addr          *net.UDPAddr
	peerID        uint16
	lastSeen      time.Time
	authenticated bool
	p2p           bool
	name          string
	ip            string
}

type udpSession struct {
	lobbyID string
	key     string
	peerID  uint16
}

type store struct {
	mu           sync.Mutex
	lobbies      map[string]*lobby
	endpoints    map[string]udpSession
	relayAddress string
}

type createRequest struct {
	Name                string `json:"name"`
	HostName            string `json:"hostName"`
	Map                 string `json:"map"`
	MaxPlayers          int    `json:"maxPlayers"`
	HostPort            int    `json:"hostPort"`
	PVP                 bool   `json:"pvp"`
	CanGrab             bool   `json:"canGrab"`
	GrabOnlyUnconscious bool   `json:"grabOnlyUnconscious"`
	AllowRespawn        bool   `json:"allowRespawn"`
	RespawnTime         int    `json:"respawnTime"`
	RespawnAtStart      bool   `json:"respawnAtStart"`
	ConnectionMode      string `json:"connectionMode"`
	ModVersion          string `json:"modVersion"`
}

type heartbeatRequest struct {
	Players int    `json:"players"`
	Map     string `json:"map"`
}

type joinRequest struct {
	PlayerName string `json:"playerName"`
	ModVersion string `json:"modVersion"`
}

type banRequest struct {
	PlayerName      string `json:"playerName"`
	DurationMinutes int    `json:"durationMinutes"`
}

func randomHex(bytes int) string {
	return hex.EncodeToString(randomBytes(bytes))
}

func randomBytes(length int) []byte {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func bearer(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}

func (s *store) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, l := range s.lobbies {
		if now.Sub(l.UpdatedAt) > lobbyTTL {
			s.deleteLobbyLocked(id)
			continue
		}

		for key, p := range l.peers {
			ttl := pendingPeerTTL
			if p.authenticated {
				ttl = peerTTL
			}
			if now.Sub(p.lastSeen) > ttl {
				s.deletePeerLocked(l, key)
			}
		}
		for ip, expiresAt := range l.bannedIPs {
			if !expiresAt.After(now) {
				delete(l.bannedIPs, ip)
			}
		}
	}
}

func (s *store) deletePeerLocked(l *lobby, key string) {
	p, ok := l.peers[key]
	if !ok {
		return
	}
	if p.addr != nil {
		delete(s.endpoints, p.addr.String())
	}
	delete(l.peers, key)
}

func (s *store) deleteLobbyLocked(id string) {
	for endpoint, session := range s.endpoints {
		if session.lobbyID == id {
			delete(s.endpoints, endpoint)
		}
	}
	delete(s.lobbies, id)
}

func lobbySnapshotLocked(l *lobby) lobby {
	snapshot := *l
	reserved := len(l.peers) + 1
	if snapshot.Players < reserved {
		snapshot.Players = reserved
	}
	if snapshot.Players > snapshot.MaxPlayers {
		snapshot.Players = snapshot.MaxPlayers
	}
	return snapshot
}

func normalizePlayerName(value string) string {
	name := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(value, "\r", " "), "\n", " "))
	if name == "" {
		return "Player"
	}
	runes := []rune(name)
	if len(runes) > 32 {
		return string(runes[:32])
	}
	return name
}

// normalizeModVersion keeps pre-versioned clients compatible with the last
// version that did not send modVersion in lobby requests.
func normalizeModVersion(value string) string {
	if value == "" {
		return legacyModVersion
	}
	return value
}

func requestIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}
	return host
}

func udpIP(addr *net.UDPAddr) string {
	if addr == nil || addr.IP == nil {
		return ""
	}
	return addr.IP.String()
}

func isBannedLocked(l *lobby, ip string, now time.Time) bool {
	if ip == "" {
		return false
	}
	expiresAt, banned := l.bannedIPs[ip]
	if !banned {
		return false
	}
	if !expiresAt.After(now) {
		delete(l.bannedIPs, ip)
		return false
	}
	return true
}

func playerNameTakenLocked(l *lobby, name string) bool {
	if strings.EqualFold(l.HostName, name) {
		return true
	}
	for _, p := range l.peers {
		if strings.EqualFold(p.name, name) {
			return true
		}
	}
	return false
}

func main() {
	httpAddress := flag.String("http", ":8080", "HTTP listen address")
	udpAddress := flag.String("udp", ":27015", "UDP relay listen address")
	legacyTCPAddress := flag.String("tcp", "", "Deprecated alias for -udp")
	relayPublicAddress := flag.String("relay-public", "udp://gunsawudp.e621.su:27015", "Public UDP relay address returned to clients")
	flag.Parse()
	if *legacyTCPAddress != "" {
		*udpAddress = *legacyTCPAddress
	}
	if !strings.Contains(*relayPublicAddress, "://") {
		*relayPublicAddress = "udp://" + *relayPublicAddress
	}

	s := &store{
		lobbies:      make(map[string]*lobby),
		endpoints:    make(map[string]udpSession),
		relayAddress: *relayPublicAddress,
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.cleanup()
		}
	}()
	go runUDPRelay(s, *udpAddress)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "protocol": protocolVersion})
	})
	mux.HandleFunc("/v1/lobbies", s.handleLobbies)
	mux.HandleFunc("/v1/lobbies/", s.handleLobby)
	log.Printf("HTTP lobby directory listening on %s; UDP relay on %s; public relay %s", *httpAddress, *udpAddress, *relayPublicAddress)
	log.Fatal(http.ListenAndServe(*httpAddress, mux))
}

func (s *store) handleLobbies(w http.ResponseWriter, r *http.Request) {
	s.cleanup()
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		list := make([]lobby, 0, len(s.lobbies))
		for _, l := range s.lobbies {
			list = append(list, lobbySnapshotLocked(l))
		}
		s.mu.Unlock()
		writeJSON(w, 200, list)
	case http.MethodPost:
		var in createRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			fail(w, 400, "invalid lobby fields")
			return
		}
		in.ModVersion = normalizeModVersion(in.ModVersion)
		if len(in.Name) < 1 || len(in.Name) > 48 || len(in.HostName) > 32 || len(in.Map) > 64 || len(in.ModVersion) > 32 || in.MaxPlayers < 1 || in.MaxPlayers > 16 || in.HostPort < 1 || in.HostPort > 65535 || in.RespawnTime < 0 || in.RespawnTime > 3600 {
			fail(w, 400, "invalid lobby fields")
			return
		}
		connectionMode := "Relay"
		if strings.EqualFold(in.ConnectionMode, "P2P") {
			connectionMode = "P2P"
		} else if strings.EqualFold(in.ConnectionMode, "Auto") {
			connectionMode = "Auto"
		}
		l := &lobby{
			ID: randomHex(16), Name: in.Name, HostName: normalizePlayerName(in.HostName), Map: in.Map,
			MaxPlayers: in.MaxPlayers, Players: 1, PVP: in.PVP, CanGrab: in.CanGrab,
			GrabOnlyUnconscious: in.CanGrab && in.GrabOnlyUnconscious,
			AllowRespawn:        in.AllowRespawn, RespawnTime: in.RespawnTime,
			RespawnAtStart: in.RespawnAtStart, ConnectionMode: connectionMode, HostPort: in.HostPort, ModVersion: in.ModVersion,
			UpdatedAt: time.Now(), HostKey: randomHex(16), HostPeer: 1, P2PKey: randomBytes(p2pKeySize),
			peers: make(map[string]peer), bannedIPs: make(map[string]time.Time),
			usedPeerIDs: map[uint16]bool{1: true},
		}
		s.mu.Lock()
		s.lobbies[l.ID] = l
		s.mu.Unlock()
		s.mu.Lock()
		snapshot := lobbySnapshotLocked(l)
		s.mu.Unlock()
		writeJSON(w, 201, map[string]any{"id": l.ID, "lobby": snapshot, "hostRelayKey": l.HostKey, "hostPeerId": l.HostPeer, "relayAddress": s.relayAddress})
	default:
		fail(w, 405, "method not allowed")
	}
}

func (s *store) handleLobby(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/lobbies/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		fail(w, 404, "not found")
		return
	}
	id := parts[0]
	s.mu.Lock()
	l := s.lobbies[id]
	s.mu.Unlock()
	if l == nil {
		fail(w, 404, "lobby expired or not found")
		return
	}
	if len(parts) == 2 && parts[1] == "join" && r.Method == http.MethodPost {
		var in joinRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			fail(w, 400, "invalid join request")
			return
		}
		in.ModVersion = normalizeModVersion(in.ModVersion)
		playerName := normalizePlayerName(in.PlayerName)
		ip := requestIP(r)
		s.mu.Lock()
		if in.ModVersion != l.ModVersion {
			s.mu.Unlock()
			fail(w, 426, "mod version does not match this lobby")
			return
		}
		if isBannedLocked(l, ip, time.Now()) {
			s.mu.Unlock()
			fail(w, 403, "you are temporarily banned from this lobby")
			return
		}
		if playerNameTakenLocked(l, playerName) {
			s.mu.Unlock()
			fail(w, 409, "a player with this name is already in the lobby")
			return
		}
		if len(l.peers)+1 >= l.MaxPlayers {
			s.mu.Unlock()
			fail(w, 409, "lobby is full")
			return
		}
		key := randomHex(16)
		peerID := nextPeerID(l)
		if peerID == 0 {
			s.mu.Unlock()
			fail(w, 409, "lobby has no peer IDs available")
			return
		}
		if l.usedPeerIDs == nil {
			l.usedPeerIDs = map[uint16]bool{l.HostPeer: true}
		}
		l.peers[key] = peer{key: key, peerID: peerID, name: playerName, ip: ip, lastSeen: time.Now()}
		l.usedPeerIDs[peerID] = true
		snapshot := lobbySnapshotLocked(l)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"id": l.ID, "lobby": snapshot, "relayKey": key, "peerId": peerID, "relayAddress": s.relayAddress})
		return
	}
	if bearer(r) != l.HostKey {
		fail(w, 401, "host authorization required")
		return
	}
	if len(parts) == 2 && parts[1] == "ban" && r.Method == http.MethodPost {
		var in banRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			fail(w, 400, "invalid ban request")
			return
		}
		playerName := normalizePlayerName(in.PlayerName)
		if strings.EqualFold(l.HostName, playerName) {
			fail(w, 400, "why u wanna ban yourself lol")
			return
		}
		duration := defaultBanTTL
		if in.DurationMinutes > 0 {
			duration = time.Duration(in.DurationMinutes) * time.Minute
		}
		if duration > maxBanTTL {
			duration = maxBanTTL
		}
		s.mu.Lock()
		var target peer
		found := false
		for _, p := range l.peers {
			if strings.EqualFold(p.name, playerName) {
				target = p
				found = true
				break
			}
		}
		if !found || target.ip == "" {
			s.mu.Unlock()
			fail(w, 404, "player not found")
			return
		}
		expiresAt := time.Now().Add(duration)
		if l.bannedIPs == nil {
			l.bannedIPs = make(map[string]time.Time)
		}
		l.bannedIPs[target.ip] = expiresAt
		s.deletePeerLocked(l, target.key)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "banned", "peerId": target.peerID,
			"expiresAt": expiresAt.UTC().Format(time.RFC3339)})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var in heartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Players < 1 || in.Players > l.MaxPlayers || len(in.Map) > 64 {
			fail(w, 400, "invalid player count")
			return
		}
		s.mu.Lock()
		l.Players = in.Players
		if in.Map != "" {
			l.Map = in.Map
		}
		l.UpdatedAt = time.Now()
		s.mu.Unlock()
		writeJSON(w, 200, map[string]string{"status": "ok"})
	case http.MethodDelete:
		s.mu.Lock()
		s.deleteLobbyLocked(id)
		s.mu.Unlock()
		w.WriteHeader(204)
	default:
		fail(w, 405, "method not allowed")
	}
}

func nextPeerID(l *lobby) uint16 {
	if l.usedPeerIDs == nil {
		l.usedPeerIDs = map[uint16]bool{l.HostPeer: true}
		for _, p := range l.peers {
			l.usedPeerIDs[p.peerID] = true
		}
	}
	for id := uint16(2); id < 65535; id++ {
		if !l.usedPeerIDs[id] {
			return id
		}
	}
	return 0
}

func runUDPRelay(s *store, address string) {
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	serveUDP(s, conn)
}

func serveUDP(s *store, conn *net.UDPConn) {
	buffer := make([]byte, maxDatagram)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		if n < 5 || !hasMagic(buffer[:n]) {
			continue
		}
		packet := append([]byte(nil), buffer[:n]...)
		switch packet[4] {
		case udpAuth:
			s.handleUDPAuth(conn, addr, packet)
		case udpData:
			s.handleUDPData(conn, addr, packet)
		case udpP2PEnable:
			s.handleUDPP2PEnable(conn, addr)
		case udpKeepAlive:
			s.handleUDPKeepAlive(addr)
		}
	}
}

func hasMagic(packet []byte) bool {
	return len(packet) >= 4 && packet[0] == udpMagic[0] && packet[1] == udpMagic[1] && packet[2] == udpMagic[2] && packet[3] == udpMagic[3]
}

func (s *store) handleUDPAuth(conn *net.UDPConn, addr *net.UDPAddr, packet []byte) {
	if len(packet) != 5+64 {
		return
	}
	id := string(packet[5:37])
	key := string(packet[37:69])

	s.mu.Lock()
	l := s.lobbies[id]
	var peerID uint16
	var failureMessage string
	if l != nil && key == l.HostKey {
		peerID = l.HostPeer
		if l.HostAddr != nil {
			delete(s.endpoints, l.HostAddr.String())
		}
		l.HostAddr = cloneUDPAddr(addr)
		l.HostP2P = false
	} else if l != nil {
		p, ok := l.peers[key]
		if isBannedLocked(l, udpIP(addr), time.Now()) {
			failureMessage = "You are temporarily banned from this lobby."
		} else if ok {
			peerID = p.peerID
			if p.addr != nil {
				delete(s.endpoints, p.addr.String())
			}
			p.addr = cloneUDPAddr(addr)
			p.ip = udpIP(addr)
			p.lastSeen = time.Now()
			p.authenticated = true
			p.p2p = false
			l.peers[key] = p
		}
	}
	if peerID != 0 {
		s.endpoints[addr.String()] = udpSession{lobbyID: id, key: key, peerID: peerID}
	}
	var p2pKey []byte
	if peerID != 0 {
		p2pKey = append([]byte(nil), l.P2PKey...)
	}
	s.mu.Unlock()

	responseType := udpAuthOK
	if peerID == 0 {
		responseType = udpAuthFailed
	}
	response := []byte{udpMagic[0], udpMagic[1], udpMagic[2], udpMagic[3], responseType, byte(peerID), byte(peerID >> 8)}
	if responseType == udpAuthOK && len(p2pKey) == p2pKeySize {
		response = append(response, p2pKey...)
	} else if responseType == udpAuthFailed && failureMessage != "" {
		response = append(response, []byte(failureMessage)...)
	}
	_, _ = conn.WriteToUDP(response, addr)
}

func (s *store) handleUDPP2PEnable(conn *net.UDPConn, addr *net.UDPAddr) {
	s.mu.Lock()
	session, l, ok := s.validSessionLocked(addr)
	if !ok {
		s.mu.Unlock()
		return
	}
	if session.key == l.HostKey {
		l.HostP2P = true
	} else {
		p := l.peers[session.key]
		p.p2p = true
		p.lastSeen = time.Now()
		l.peers[session.key] = p
	}
	notices := candidateNoticesLocked(l)
	s.mu.Unlock()
	for _, notice := range notices {
		_, _ = conn.WriteToUDP(notice.packet, notice.target)
	}
}

func (s *store) handleUDPKeepAlive(addr *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, l, ok := s.validSessionLocked(addr)
	if !ok || session.key == l.HostKey {
		return
	}
	p := l.peers[session.key]
	p.lastSeen = time.Now()
	l.peers[session.key] = p
}

func (s *store) validSessionLocked(addr *net.UDPAddr) (udpSession, *lobby, bool) {
	session, ok := s.endpoints[addr.String()]
	if !ok {
		return udpSession{}, nil, false
	}
	l := s.lobbies[session.lobbyID]
	if l == nil {
		return udpSession{}, nil, false
	}
	if session.key == l.HostKey {
		return session, l, l.HostAddr != nil && l.HostAddr.String() == addr.String()
	}
	p, exists := l.peers[session.key]
	return session, l, exists && p.addr != nil && p.addr.String() == addr.String() && p.peerID == session.peerID
}

type candidateNotice struct {
	target *net.UDPAddr
	packet []byte
}

func candidateNoticesLocked(l *lobby) []candidateNotice {
	type endpoint struct {
		id   uint16
		addr *net.UDPAddr
		p2p  bool
	}
	endpoints := []endpoint{{l.HostPeer, l.HostAddr, l.HostP2P}}
	for _, p := range l.peers {
		endpoints = append(endpoints, endpoint{p.peerID, p.addr, p.p2p})
	}
	result := make([]candidateNotice, 0, len(endpoints)*len(endpoints))
	for _, target := range endpoints {
		if !target.p2p || target.addr == nil {
			continue
		}
		for _, peer := range endpoints {
			if peer.id == target.id || !peer.p2p || peer.addr == nil {
				continue
			}
			packet := candidatePacket(peer.id, peer.addr)
			if packet != nil {
				result = append(result, candidateNotice{cloneUDPAddr(target.addr), packet})
			}
		}
	}
	return result
}

func candidatePacket(peerID uint16, addr *net.UDPAddr) []byte {
	ip := addr.IP.To4()
	if ip == nil {
		ip = addr.IP.To16()
	}
	if ip == nil {
		return nil
	}
	packet := make([]byte, 5+2+2+1+len(ip))
	copy(packet[:4], udpMagic)
	packet[4] = udpCandidate
	packet[5] = byte(peerID)
	packet[6] = byte(peerID >> 8)
	packet[7] = byte(addr.Port)
	packet[8] = byte(addr.Port >> 8)
	packet[9] = byte(len(ip))
	copy(packet[10:], ip)
	return packet
}

func (s *store) handleUDPData(conn *net.UDPConn, addr *net.UDPAddr, packet []byte) {
	if len(packet) < 5+2+4+2+2+4 {
		return
	}
	targetID := uint16(packet[5]) | uint16(packet[6])<<8

	s.mu.Lock()
	session, ok := s.endpoints[addr.String()]
	l := s.lobbies[session.lobbyID]
	if !ok || l == nil {
		s.mu.Unlock()
		return
	}
	if session.key == l.HostKey {
		if l.HostAddr == nil || l.HostAddr.String() != addr.String() {
			s.mu.Unlock()
			return
		}
	} else {
		p, exists := l.peers[session.key]
		if !exists || p.addr == nil || p.addr.String() != addr.String() || p.peerID != session.peerID {
			s.mu.Unlock()
			return
		}
		p.lastSeen = time.Now()
		l.peers[session.key] = p
	}

	targets := collectTargets(l, session.peerID, targetID)
	s.mu.Unlock()
	if len(targets) == 0 {
		return
	}

	forwarded := make([]byte, 5+2+len(packet)-7)
	copy(forwarded[:4], udpMagic)
	forwarded[4] = udpForwarded
	forwarded[5] = byte(session.peerID)
	forwarded[6] = byte(session.peerID >> 8)
	copy(forwarded[7:], packet[7:])
	for _, target := range targets {
		_, _ = conn.WriteToUDP(forwarded, target)
	}
}

func collectTargets(l *lobby, senderID, targetID uint16) []*net.UDPAddr {
	result := make([]*net.UDPAddr, 0, len(l.peers)+1)
	add := func(peerID uint16, addr *net.UDPAddr) {
		if addr == nil || peerID == senderID {
			return
		}
		if targetID == 0 || targetID == peerID {
			result = append(result, cloneUDPAddr(addr))
		}
	}
	add(l.HostPeer, l.HostAddr)
	for _, p := range l.peers {
		add(p.peerID, p.addr)
	}
	return result
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	ip := append(net.IP(nil), addr.IP...)
	return &net.UDPAddr{IP: ip, Port: addr.Port, Zone: addr.Zone}
}
