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
	protocolVersion = "2"
	lobbyTTL        = 45 * time.Second
	maxDatagram     = 1400

	udpAuth       = byte(1)
	udpAuthOK     = byte(2)
	udpData       = byte(3)
	udpForwarded  = byte(4)
	udpAuthFailed = byte(5)
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
	HostPort            int          `json:"hostPort"`
	HostIP              string       `json:"hostIp"`
	UpdatedAt           time.Time    `json:"-"`
	HostKey             string       `json:"-"`
	HostPeer            uint16       `json:"-"`
	HostAddr            *net.UDPAddr `json:"-"`
	peers               map[string]peer
}

type peer struct {
	key      string
	addr     *net.UDPAddr
	peerID   uint16
	lastSeen time.Time
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
}

type heartbeatRequest struct {
	Players int    `json:"players"`
	Map     string `json:"map"`
}

func randomHex(bytes int) string {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
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
	for id, l := range s.lobbies {
		if time.Since(l.UpdatedAt) <= lobbyTTL {
			continue
		}
		for endpoint, session := range s.endpoints {
			if session.lobbyID == id {
				delete(s.endpoints, endpoint)
			}
		}
		delete(s.lobbies, id)
	}
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
			list = append(list, *l)
		}
		s.mu.Unlock()
		writeJSON(w, 200, list)
	case http.MethodPost:
		var in createRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || len(in.Name) < 1 || len(in.Name) > 48 || len(in.HostName) > 32 || len(in.Map) > 64 || in.MaxPlayers < 1 || in.MaxPlayers > 16 || in.HostPort < 1 || in.HostPort > 65535 || in.RespawnTime < 0 || in.RespawnTime > 3600 {
			fail(w, 400, "invalid lobby fields")
			return
		}
		l := &lobby{
			ID: randomHex(16), Name: in.Name, HostName: in.HostName, Map: in.Map,
			MaxPlayers: in.MaxPlayers, Players: 1, PVP: in.PVP, CanGrab: in.CanGrab,
			GrabOnlyUnconscious: in.CanGrab && in.GrabOnlyUnconscious,
			AllowRespawn:        in.AllowRespawn, RespawnTime: in.RespawnTime,
			RespawnAtStart: in.RespawnAtStart, HostPort: in.HostPort,
			UpdatedAt: time.Now(), HostKey: randomHex(16), HostPeer: 1,
			peers: make(map[string]peer),
		}
		s.mu.Lock()
		s.lobbies[l.ID] = l
		s.mu.Unlock()
		writeJSON(w, 201, map[string]any{"id": l.ID, "lobby": l, "hostRelayKey": l.HostKey, "hostPeerId": l.HostPeer, "relayAddress": s.relayAddress})
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
		s.mu.Lock()
		if len(l.peers)+1 >= l.MaxPlayers {
			s.mu.Unlock()
			fail(w, 409, "lobby is full")
			return
		}
		key := randomHex(16)
		peerID := nextPeerID(l)
		l.peers[key] = peer{key: key, peerID: peerID, lastSeen: time.Now()}
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"id": l.ID, "lobby": l, "relayKey": key, "peerId": peerID, "relayAddress": s.relayAddress})
		return
	}
	if bearer(r) != l.HostKey {
		fail(w, 401, "host authorization required")
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
		for endpoint, session := range s.endpoints {
			if session.lobbyID == id {
				delete(s.endpoints, endpoint)
			}
		}
		delete(s.lobbies, id)
		s.mu.Unlock()
		w.WriteHeader(204)
	default:
		fail(w, 405, "method not allowed")
	}
}

func nextPeerID(l *lobby) uint16 {
	used := map[uint16]bool{l.HostPeer: true}
	for _, p := range l.peers {
		used[p.peerID] = true
	}
	for id := uint16(2); id < 65535; id++ {
		if !used[id] {
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
	if l != nil && key == l.HostKey {
		peerID = l.HostPeer
		if l.HostAddr != nil {
			delete(s.endpoints, l.HostAddr.String())
		}
		l.HostAddr = cloneUDPAddr(addr)
	} else if l != nil {
		p, ok := l.peers[key]
		if ok {
			peerID = p.peerID
			if p.addr != nil {
				delete(s.endpoints, p.addr.String())
			}
			p.addr = cloneUDPAddr(addr)
			p.lastSeen = time.Now()
			l.peers[key] = p
		}
	}
	if peerID != 0 {
		s.endpoints[addr.String()] = udpSession{lobbyID: id, key: key, peerID: peerID}
	}
	s.mu.Unlock()

	responseType := udpAuthOK
	if peerID == 0 {
		responseType = udpAuthFailed
	}
	response := []byte{udpMagic[0], udpMagic[1], udpMagic[2], udpMagic[3], responseType, byte(peerID), byte(peerID >> 8)}
	_, _ = conn.WriteToUDP(response, addr)
}

func (s *store) handleUDPData(conn *net.UDPConn, addr *net.UDPAddr, packet []byte) {
	// magic + type + target id + transport fragment fields
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

	// Replace target ID with actual sender ID. The rest is transport-fragment metadata and data.
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
