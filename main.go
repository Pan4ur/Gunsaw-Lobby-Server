package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	protocolVersion     = "2"
	lobbyTTL            = 45 * time.Second
	guestReservationTTL = 15 * time.Second
	maxFrame            = 64 * 1024
)

type lobby struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	HostName            string    `json:"hostName"`
	Map                 string    `json:"map"`
	MaxPlayers          int       `json:"maxPlayers"`
	Players             int       `json:"players"`
	PVP                 bool      `json:"pvp"`
	CanGrab             bool      `json:"canGrab"`
	GrabOnlyUnconscious bool      `json:"grabOnlyUnconscious"`
	AllowRespawn        bool      `json:"allowRespawn"`
	RespawnTime         int       `json:"respawnTime"`
	RespawnAtStart      bool      `json:"respawnAtStart"`
	HostPort            int       `json:"hostPort"`
	HostIP              string    `json:"hostIp"`
	UpdatedAt           time.Time `json:"-"`
	HostKey             string    `json:"-"`
	HostPeer            uint16    `json:"-"`
	HostConn            *wsConn   `json:"-"`
	peers               map[string]peer
}

type peer struct {
	key      string
	conn     *wsConn
	peerID   uint16
	lastSeen time.Time
}

type store struct {
	mu           sync.Mutex
	lobbies      map[string]*lobby
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
	var expired []*lobby
	now := time.Now()
	s.mu.Lock()
	for id, l := range s.lobbies {
		if now.Sub(l.UpdatedAt) > lobbyTTL {
			delete(s.lobbies, id)
			expired = append(expired, l)
			continue
		}
		expireGuestReservationsLocked(l, now)
	}
	s.mu.Unlock()
	for _, l := range expired {
		closeLobbyConnections(l)
	}
}

func main() {
	httpAddress := flag.String("http", ":8080", "HTTP and WebSocket listen address")
	relayPublicAddress := flag.String("relay-public", "", "Public ws:// or wss:// relay URL returned to clients")
	flag.Parse()
	if *relayPublicAddress == "" {
		log.Fatal("-relay-public must be set, for example wss://mp.example.com/ws")
	}
	if !strings.HasPrefix(*relayPublicAddress, "ws://") && !strings.HasPrefix(*relayPublicAddress, "wss://") {
		log.Fatal("-relay-public must be a ws:// or wss:// URL")
	}

	s := &store{lobbies: make(map[string]*lobby), relayAddress: *relayPublicAddress}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.cleanup()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "protocol": protocolVersion, "transport": "websocket"})
	})
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/v1/lobbies", s.handleLobbies)
	mux.HandleFunc("/v1/lobbies/", s.handleLobby)
	log.Printf("HTTP lobby directory and WebSocket relay listening on %s; public relay %s", *httpAddress, *relayPublicAddress)
	log.Fatal(http.ListenAndServe(*httpAddress, mux))
}

func (s *store) handleLobbies(w http.ResponseWriter, r *http.Request) {
	s.cleanup()
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		list := make([]lobby, 0, len(s.lobbies))
		for _, l := range s.lobbies {
			list = append(list, lobbySnapshot(l))
		}
		s.mu.Unlock()
		writeJSON(w, 200, list)
	case http.MethodPost:
		var in createRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || len(in.Name) < 1 || len(in.Name) > 48 || len(in.HostName) > 32 || len(in.Map) > 64 || in.MaxPlayers < 2 || in.MaxPlayers > 16 || in.HostPort < 1 || in.HostPort > 65535 || in.RespawnTime < 0 || in.RespawnTime > 3600 {
			fail(w, 400, "invalid lobby fields")
			return
		}
		l := &lobby{ID: randomHex(16), Name: in.Name, HostName: in.HostName, Map: in.Map, MaxPlayers: in.MaxPlayers, PVP: in.PVP, CanGrab: in.CanGrab, GrabOnlyUnconscious: in.CanGrab && in.GrabOnlyUnconscious, AllowRespawn: in.AllowRespawn, RespawnTime: in.RespawnTime, RespawnAtStart: in.RespawnAtStart, HostPort: in.HostPort, UpdatedAt: time.Now(), HostKey: randomHex(16), HostPeer: 1, peers: make(map[string]peer)}
		s.mu.Lock()
		s.lobbies[l.ID] = l
		result := map[string]any{"id": l.ID, "lobby": lobbySnapshot(l), "hostRelayKey": l.HostKey, "hostPeerId": l.HostPeer, "relayAddress": s.relayAddress}
		s.mu.Unlock()
		writeJSON(w, 201, result)
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
	if len(parts) == 2 && parts[1] == "join" && r.Method == http.MethodPost {
		s.mu.Lock()
		l := s.lobbies[id]
		if l == nil {
			s.mu.Unlock()
			fail(w, 404, "lobby expired or not found")
			return
		}
		expireGuestReservationsLocked(l, time.Now())
		peerID := nextGuestPeerIDLocked(l)
		if peerID == 0 {
			s.mu.Unlock()
			fail(w, 409, "lobby is full")
			return
		}
		key := randomHex(16)
		l.peers[key] = peer{key: key, peerID: peerID, lastSeen: time.Now()}
		result := map[string]any{"id": l.ID, "lobby": lobbySnapshot(l), "relayKey": key, "peerId": peerID, "relayAddress": s.relayAddress}
		s.mu.Unlock()
		writeJSON(w, 200, result)
		return
	}
	switch r.Method {
	case http.MethodPut:
		var in heartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Players < 1 || len(in.Map) > 64 {
			fail(w, 400, "invalid player count")
			return
		}
		s.mu.Lock()
		l := s.lobbies[id]
		if l == nil {
			s.mu.Unlock()
			fail(w, 404, "lobby expired or not found")
			return
		}
		if bearer(r) != l.HostKey {
			s.mu.Unlock()
			fail(w, 401, "host authorization required")
			return
		}
		if in.Players > l.MaxPlayers {
			s.mu.Unlock()
			fail(w, 400, "invalid player count")
			return
		}
		if in.Map != "" {
			l.Map = in.Map
		}
		l.UpdatedAt = time.Now()
		updatePlayersLocked(l)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]string{"status": "ok"})
	case http.MethodDelete:
		s.mu.Lock()
		l := s.lobbies[id]
		if l == nil {
			s.mu.Unlock()
			fail(w, 404, "lobby expired or not found")
			return
		}
		if bearer(r) != l.HostKey {
			s.mu.Unlock()
			fail(w, 401, "host authorization required")
			return
		}
		removed := s.lobbies[id]
		delete(s.lobbies, id)
		s.mu.Unlock()
		if removed != nil {
			closeLobbyConnections(removed)
		}
		w.WriteHeader(204)
	default:
		s.mu.Lock()
		l := s.lobbies[id]
		authorized := l != nil && bearer(r) == l.HostKey
		s.mu.Unlock()
		if l == nil {
			fail(w, 404, "lobby expired or not found")
			return
		}
		if !authorized {
			fail(w, 401, "host authorization required")
			return
		}
		fail(w, 405, "method not allowed")
	}
}

func lobbySnapshot(l *lobby) lobby {
	return lobby{ID: l.ID, Name: l.Name, HostName: l.HostName, Map: l.Map, MaxPlayers: l.MaxPlayers, Players: l.Players, PVP: l.PVP, CanGrab: l.CanGrab, GrabOnlyUnconscious: l.GrabOnlyUnconscious, AllowRespawn: l.AllowRespawn, RespawnTime: l.RespawnTime, RespawnAtStart: l.RespawnAtStart, HostPort: l.HostPort, HostIP: l.HostIP, UpdatedAt: l.UpdatedAt, HostKey: l.HostKey, HostPeer: l.HostPeer}
}

func expireGuestReservationsLocked(l *lobby, now time.Time) {
	for key, p := range l.peers {
		if p.conn == nil && now.Sub(p.lastSeen) >= guestReservationTTL {
			delete(l.peers, key)
		}
	}
}

func nextGuestPeerIDLocked(l *lobby) uint16 {
	used := make(map[uint16]bool, len(l.peers))
	for _, p := range l.peers {
		used[p.peerID] = true
	}
	for id := uint16(2); int(id) <= l.MaxPlayers; id++ {
		if !used[id] {
			return id
		}
	}
	return 0
}

func updatePlayersLocked(l *lobby) {
	players := 0
	if l.HostConn != nil {
		players++
	}
	for _, p := range l.peers {
		if p.conn != nil {
			players++
		}
	}
	l.Players = players
}

func (s *store) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		log.Printf("websocket upgrade failed from %s: %v", r.RemoteAddr, err)
		return
	}
	go s.relayConnection(conn)
}

func (s *store) relayConnection(conn *wsConn) {
	defer conn.Close()
	auth, err := conn.ReadMessage()
	if err != nil || len(auth) != 64 {
		return
	}
	id, key := string(auth[:32]), string(auth[32:])
	var replaced *wsConn
	s.mu.Lock()
	l := s.lobbies[id]
	if l == nil {
		s.mu.Unlock()
		return
	}
	if key == l.HostKey {
		if l.HostConn != nil {
			replaced = l.HostConn
		}
		l.HostConn = conn
	} else if p, ok := l.peers[key]; ok {
		if p.conn == nil && time.Since(p.lastSeen) >= guestReservationTTL {
			delete(l.peers, key)
			s.mu.Unlock()
			return
		}
		if p.conn != nil {
			replaced = p.conn
		}
		p.conn = conn
		p.lastSeen = time.Now()
		l.peers[key] = p
	} else {
		s.mu.Unlock()
		return
	}
	updatePlayersLocked(l)
	s.mu.Unlock()
	if replaced != nil {
		_ = replaced.Close()
	}
	defer s.disconnectRelay(id, key, conn)

	for {
		message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		s.forwardRelayFrom(id, key, conn, message)
	}
}

func (s *store) disconnectRelay(id, key string, conn *wsConn) {
	s.mu.Lock()
	var senderID uint16
	var targets []*wsConn
	if l := s.lobbies[id]; l != nil {
		if key == l.HostKey && l.HostConn == conn {
			senderID = 1
			l.HostConn = nil
		} else if p, ok := l.peers[key]; ok && p.conn == conn {
			senderID = p.peerID
			delete(l.peers, key)
		}
		updatePlayersLocked(l)
		if senderID != 0 {
			if l.HostConn != nil {
				targets = append(targets, l.HostConn)
			}
			for _, p := range l.peers {
				if p.conn != nil {
					targets = append(targets, p.conn)
				}
			}
		}
	}
	s.mu.Unlock()
	if senderID == 0 {
		return
	}
	message := []byte{0, 0, 'G', 'M', 'P', '1', 0x14, 1}
	binary.LittleEndian.PutUint16(message, senderID)
	for _, target := range targets {
		if err := target.WriteMessage(message); err != nil {
			_ = target.Close()
		}
	}
}

func (s *store) forwardRelay(id, key string, message []byte) {
	s.forwardRelayFrom(id, key, nil, message)
}

func (s *store) forwardRelayFrom(id, key string, source *wsConn, message []byte) {
	if len(message) < 2 {
		return
	}
	s.mu.Lock()
	l := s.lobbies[id]
	if l == nil {
		s.mu.Unlock()
		return
	}
	var senderID uint16
	if key == l.HostKey {
		if source != nil && l.HostConn != source {
			s.mu.Unlock()
			return
		}
		senderID = 1
	} else if p, ok := l.peers[key]; ok && (source == nil || p.conn == source) {
		senderID = p.peerID
	} else {
		s.mu.Unlock()
		return
	}

	targetID := binary.LittleEndian.Uint16(message[:2])
	targets := make([]*wsConn, 0, len(l.peers)+1)
	if targetID == 0 {
		if senderID != 1 && l.HostConn != nil {
			targets = append(targets, l.HostConn)
		}
		for _, p := range l.peers {
			if p.conn != nil && p.peerID != senderID {
				targets = append(targets, p.conn)
			}
		}
	} else if targetID == 1 {
		if l.HostConn != nil {
			targets = append(targets, l.HostConn)
		}
	} else {
		for _, p := range l.peers {
			if p.peerID == targetID && p.conn != nil {
				targets = append(targets, p.conn)
				break
			}
		}
	}
	s.mu.Unlock()

	relayMessage := make([]byte, len(message))
	binary.LittleEndian.PutUint16(relayMessage, senderID)
	copy(relayMessage[2:], message[2:])
	for _, target := range targets {
		if err := target.WriteMessage(relayMessage); err != nil {
			_ = target.Close()
		}
	}
}

const webSocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	conn      net.Conn
	reader    *bufio.Reader
	writeMu   sync.Mutex
	closeOnce sync.Once
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if r.Method != http.MethodGet {
		return nil, errors.New("websocket requires GET")
	}
	if !headerContainsToken(r.Header, "Connection", "upgrade") ||
		!headerContainsToken(r.Header, "Upgrade", "websocket") {
		return nil, errors.New("missing websocket upgrade headers")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, errors.New("unsupported websocket version")
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("HTTP server does not support hijacking")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	acceptHash := sha1.Sum([]byte(key + webSocketGUID))
	accept := base64.StdEncoding.EncodeToString(acceptHash[:])
	_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
	if err == nil {
		err = rw.Flush()
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, reader: rw.Reader}, nil
}

func headerContainsToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (c *wsConn) ReadMessage() ([]byte, error) {
	var message []byte
	started := false
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x8:
			_ = c.writeFrame(true, 0x8, payload)
			return nil, io.EOF
		case 0x9:
			if err := c.writeFrame(true, 0xA, payload); err != nil {
				return nil, err
			}
			continue
		case 0xA:
			continue
		case 0x2:
			if started {
				return nil, errors.New("unexpected binary frame")
			}
			started = true
			message = append(message, payload...)
		case 0x0:
			if !started {
				return nil, errors.New("unexpected continuation frame")
			}
			message = append(message, payload...)
		default:
			return nil, errors.New("only binary websocket messages are supported")
		}
		if len(message) > maxFrame {
			return nil, errors.New("websocket message too large")
		}
		if fin {
			return message, nil
		}
	}
}

func (c *wsConn) WriteMessage(message []byte) error {
	if len(message) > maxFrame {
		return errors.New("websocket message too large")
	}
	return c.writeFrame(true, 0x2, message)
}

func (c *wsConn) readFrame() (bool, byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return false, 0, nil, err
	}
	fin := header[0]&0x80 != 0
	if header[0]&0x70 != 0 {
		return false, 0, nil, errors.New("unsupported websocket extensions")
	}
	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	if !masked {
		return false, 0, nil, errors.New("client websocket frame is not masked")
	}
	length := uint64(header[1] & 0x7F)
	if length == 126 {
		var extended [2]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	} else if length == 127 {
		var extended [8]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(extended[:])
	}
	if opcode >= 0x8 && (!fin || length > 125) {
		return false, 0, nil, errors.New("invalid websocket control frame")
	}
	if length > maxFrame {
		return false, 0, nil, errors.New("websocket frame too large")
	}
	var mask [4]byte
	if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
		return false, 0, nil, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return false, 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return fin, opcode, payload, nil
}

func (c *wsConn) writeFrame(fin bool, opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	first := opcode
	if fin {
		first |= 0x80
	}
	header := []byte{first}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0,
			byte(uint64(len(payload))>>24), byte(uint64(len(payload))>>16),
			byte(uint64(len(payload))>>8), byte(len(payload)))
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

func (c *wsConn) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if c.conn == nil {
			return
		}
		_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = c.conn.Write([]byte{0x88, 0x00})
		closeErr = c.conn.Close()
	})
	return closeErr
}

func closeLobbyConnections(l *lobby) {
	if l.HostConn != nil {
		_ = l.HostConn.Close()
	}
	for _, p := range l.peers {
		if p.conn != nil {
			_ = p.conn.Close()
		}
	}
}
