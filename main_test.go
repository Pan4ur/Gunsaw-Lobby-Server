package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	testLobbyID  = "0123456789abcdef0123456789abcdef"
	testHostKey  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testPeer2Key = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testPeer3Key = "cccccccccccccccccccccccccccccccc"
)

type relayFixture struct {
	t       *testing.T
	store   *store
	lobby   *lobby
	clients map[uint16]net.Conn
}

func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	l := &lobby{ID: testLobbyID, HostKey: testHostKey, HostPeer: 1, MaxPlayers: 4, UpdatedAt: time.Now(), peers: make(map[string]peer)}
	f := &relayFixture{t: t, lobby: l, store: &store{lobbies: map[string]*lobby{testLobbyID: l}}, clients: make(map[uint16]net.Conn)}
	f.addPeer(1, testHostKey)
	f.addPeer(2, testPeer2Key)
	f.addPeer(3, testPeer3Key)
	return f
}

func (f *relayFixture) addPeer(id uint16, key string) {
	f.t.Helper()
	server, client := net.Pipe()
	conn := &wsConn{conn: server, reader: bufio.NewReader(server)}
	f.clients[id] = client
	if id == 1 {
		f.lobby.HostConn = conn
	} else {
		f.lobby.peers[key] = peer{key: key, peerID: id, conn: conn, lastSeen: time.Now()}
	}
	updatePlayersLocked(f.lobby)
	f.t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
}

func outboundPacket(target uint16, payload string) []byte {
	packet := make([]byte, 2+len(payload))
	binary.LittleEndian.PutUint16(packet, target)
	copy(packet[2:], payload)
	return packet
}

func recipientPacket(sender uint16, payload string) []byte {
	return outboundPacket(sender, payload)
}

func TestProtocolVersion(t *testing.T) {
	if protocolVersion != "2" {
		t.Fatalf("protocolVersion = %q, want 2", protocolVersion)
	}
}

func TestRelayBroadcastRouting(t *testing.T) {
	t.Run("host to all guests", func(t *testing.T) {
		f := newRelayFixture(t)
		want := recipientPacket(1, "host broadcast")
		assertForwarded(t, f, testHostKey, outboundPacket(0, "host broadcast"), want, 2, 3)
	})

	t.Run("guest to host and other guests", func(t *testing.T) {
		f := newRelayFixture(t)
		want := recipientPacket(2, "guest broadcast")
		assertForwarded(t, f, testPeer2Key, outboundPacket(0, "guest broadcast"), want, 1, 3)
		assertNoMessage(t, f.clients[2])
	})
}

func TestRelayTargetRouting(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		target    uint16
		sender    uint16
		recipient uint16
	}{
		{name: "host to guest", key: testHostKey, target: 3, sender: 1, recipient: 3},
		{name: "guest to host", key: testPeer2Key, target: 1, sender: 2, recipient: 1},
		{name: "guest to guest", key: testPeer2Key, target: 3, sender: 2, recipient: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRelayFixture(t)
			assertForwarded(t, f, tt.key, outboundPacket(tt.target, "targeted"), recipientPacket(tt.sender, "targeted"), tt.recipient)
			for _, id := range []uint16{1, 2, 3} {
				if id != tt.recipient {
					assertNoMessage(t, f.clients[id])
				}
			}
		})
	}
}

func TestRelayIgnoresUnknownTargetAndShortPacket(t *testing.T) {
	for _, packet := range [][]byte{outboundPacket(4, "missing"), nil, {0x01}} {
		f := newRelayFixture(t)
		f.store.forwardRelay(testLobbyID, testPeer2Key, packet)
		for _, id := range []uint16{1, 2, 3} {
			assertNoMessage(t, f.clients[id])
		}
	}
}

func TestRelayUsesAuthenticatedSenderID(t *testing.T) {
	f := newRelayFixture(t)
	// The first uint16 is always a target. Any apparent sender ID supplied there
	// is discarded and replaced with the ID assigned to the authenticated key.
	assertForwarded(t, f, testPeer2Key, outboundPacket(3, "payload"), recipientPacket(2, "payload"), 3)
}

func TestDisconnectRelayNotifiesRemainingPeers(t *testing.T) {
	f := newRelayFixture(t)
	departed := f.lobby.peers[testPeer2Key].conn
	done := make(chan struct{})
	go func() {
		f.store.disconnectRelay(testLobbyID, testPeer2Key, departed)
		close(done)
	}()

	payload := string([]byte{'G', 'M', 'P', '1', 0x14, 1})
	want := recipientPacket(2, payload)
	for _, id := range []uint16{1, 3} {
		got, err := readServerMessage(f.clients[id])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("peer %d got %v, want %v", id, got, want)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnect notification blocked")
	}
}

func TestForwardRelayDoesNotHoldStoreMutexWhileWriting(t *testing.T) {
	f := newRelayFixture(t)
	forwarded := make(chan struct{})
	go func() {
		f.store.forwardRelay(testLobbyID, testHostKey, outboundPacket(2, "blocked until read"))
		close(forwarded)
	}()
	time.Sleep(10 * time.Millisecond)

	locked := make(chan struct{})
	go func() {
		f.store.mu.Lock()
		f.store.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("store mutex was held during WriteMessage")
	}
	if _, err := readServerMessage(f.clients[2]); err != nil {
		t.Fatal(err)
	}
	select {
	case <-forwarded:
	case <-time.After(time.Second):
		t.Fatal("forward did not finish")
	}
}

func TestJoinAllocatesMinimumFreePeerID(t *testing.T) {
	now := time.Now()
	l := &lobby{ID: testLobbyID, HostKey: testHostKey, MaxPlayers: 4, UpdatedAt: now, peers: map[string]peer{
		testPeer2Key: {key: testPeer2Key, peerID: 2, lastSeen: now},
		testPeer3Key: {key: testPeer3Key, peerID: 4, lastSeen: now},
	}}
	s := &store{lobbies: map[string]*lobby{testLobbyID: l}, relayAddress: "ws://relay"}

	response := httptest.NewRecorder()
	s.handleLobby(response, httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+testLobbyID+"/join", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body struct {
		PeerID uint16 `json:"peerId"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.PeerID != 3 {
		t.Fatalf("peerId = %d, want 3", body.PeerID)
	}
}

func TestJoinExpiresReservationsAndRejectsFullLobby(t *testing.T) {
	t.Run("expired reservation is reused", func(t *testing.T) {
		l := &lobby{ID: testLobbyID, MaxPlayers: 2, UpdatedAt: time.Now(), peers: map[string]peer{
			testPeer2Key: {key: testPeer2Key, peerID: 2, lastSeen: time.Now().Add(-guestReservationTTL)},
		}}
		s := &store{lobbies: map[string]*lobby{testLobbyID: l}}
		response := httptest.NewRecorder()
		s.handleLobby(response, httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+testLobbyID+"/join", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		var body struct {
			PeerID uint16 `json:"peerId"`
		}
		_ = json.NewDecoder(response.Body).Decode(&body)
		if body.PeerID != 2 {
			t.Fatalf("peerId = %d, want 2", body.PeerID)
		}
	})

	t.Run("active reservation fills lobby", func(t *testing.T) {
		l := &lobby{ID: testLobbyID, MaxPlayers: 2, UpdatedAt: time.Now(), peers: map[string]peer{
			testPeer2Key: {key: testPeer2Key, peerID: 2, lastSeen: time.Now()},
		}}
		s := &store{lobbies: map[string]*lobby{testLobbyID: l}}
		response := httptest.NewRecorder()
		s.handleLobby(response, httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+testLobbyID+"/join", nil))
		if response.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", response.Code)
		}
	})
}

func TestRelayConnectionTracksPlayersAndReleasesGuest(t *testing.T) {
	l := &lobby{ID: testLobbyID, HostKey: testHostKey, HostPeer: 1, MaxPlayers: 3, UpdatedAt: time.Now(), peers: map[string]peer{
		testPeer2Key: {key: testPeer2Key, peerID: 2, lastSeen: time.Now()},
	}}
	s := &store{lobbies: map[string]*lobby{testLobbyID: l}}
	host := startRelayConnection(t, s, testLobbyID+testHostKey)
	guest := startRelayConnection(t, s, testLobbyID+testPeer2Key)
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return l.Players == 2
	})

	_ = guest.Close()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, reserved := l.peers[testPeer2Key]
		return l.Players == 1 && !reserved
	})
	s.mu.Lock()
	reusedID := nextGuestPeerIDLocked(l)
	s.mu.Unlock()
	if reusedID != 2 {
		t.Fatalf("peer ID after disconnect = %d, want released ID 2", reusedID)
	}
	_ = host.Close()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return l.Players == 0
	})
}

func TestRelayRequiresExactly64AuthBytes(t *testing.T) {
	for _, size := range []int{63, 65} {
		t.Run(string(rune(size)), func(t *testing.T) {
			l := &lobby{ID: testLobbyID, HostKey: testHostKey, MaxPlayers: 2, peers: make(map[string]peer)}
			s := &store{lobbies: map[string]*lobby{testLobbyID: l}}
			client := startRelayConnection(t, s, string(bytes.Repeat([]byte{'x'}, size)))
			defer client.Close()
			waitFor(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return l.HostConn == nil && l.Players == 0
			})
		})
	}
}

func TestHeartbeatUpdatesMapButNotPlayers(t *testing.T) {
	hostServer, hostClient := net.Pipe()
	guestServer, guestClient := net.Pipe()
	defer hostServer.Close()
	defer hostClient.Close()
	defer guestServer.Close()
	defer guestClient.Close()
	l := &lobby{ID: testLobbyID, HostKey: testHostKey, MaxPlayers: 4, Players: 99, Map: "old", UpdatedAt: time.Now().Add(-time.Second), HostConn: &wsConn{conn: hostServer}, peers: map[string]peer{
		testPeer2Key: {key: testPeer2Key, peerID: 2, conn: &wsConn{conn: guestServer}},
	}}
	s := &store{lobbies: map[string]*lobby{testLobbyID: l}}
	before := l.UpdatedAt
	request := httptest.NewRequest(http.MethodPut, "/v1/lobbies/"+testLobbyID, bytes.NewBufferString(`{"players":4,"map":"new"}`))
	request.Header.Set("Authorization", "Bearer "+testHostKey)
	response := httptest.NewRecorder()
	s.handleLobby(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if l.Players != 2 {
		t.Fatalf("players = %d, want active connection count 2", l.Players)
	}
	if l.Map != "new" || !l.UpdatedAt.After(before) {
		t.Fatalf("heartbeat did not update map/time: map=%q time=%v", l.Map, l.UpdatedAt)
	}
}

func assertForwarded(t *testing.T, f *relayFixture, key string, packet, want []byte, recipients ...uint16) {
	t.Helper()
	type result struct {
		id      uint16
		message []byte
		err     error
	}
	results := make(chan result, len(recipients))
	for _, id := range recipients {
		go func(id uint16) {
			message, err := readServerMessage(f.clients[id])
			results <- result{id: id, message: message, err: err}
		}(id)
	}
	f.store.forwardRelay(testLobbyID, key, packet)
	for range recipients {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("peer %d read failed: %v", got.id, got.err)
			}
			if !bytes.Equal(got.message, want) {
				t.Fatalf("peer %d got %v, want %v", got.id, got.message, want)
			}
		case <-time.After(time.Second):
			t.Fatal("relay timed out")
		}
	}
}

func assertNoMessage(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	_, err := readServerMessage(conn)
	if err == nil {
		t.Fatal("unexpected relay message")
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func startRelayConnection(t *testing.T, s *store, auth string) net.Conn {
	t.Helper()
	server, client := net.Pipe()
	go s.relayConnection(&wsConn{conn: server, reader: bufio.NewReader(server)})
	if err := writeMaskedClientMessage(client, []byte(auth)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func writeMaskedClientMessage(w io.Writer, payload []byte) error {
	mask := [4]byte{1, 2, 3, 4}
	header := []byte{0x82}
	if len(payload) < 126 {
		header = append(header, 0x80|byte(len(payload)))
	} else {
		header = append(header, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	}
	header = append(header, mask[:]...)
	masked := append([]byte(nil), payload...)
	for i := range masked {
		masked[i] ^= mask[i%4]
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(masked)
	return err
}

func readServerMessage(r io.Reader) ([]byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := int(header[1] & 0x7f)
	if n == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return nil, err
		}
		n = int(ext[0])<<8 | int(ext[1])
	}
	data := make([]byte, n)
	_, err := io.ReadFull(r, data)
	return data, err
}
