package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJoinRejectsDuplicateNamesAndBannedIPs(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	const hostKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s := &store{
		lobbies: map[string]*lobby{id: {
			ID: id, HostName: "Host", HostKey: hostKey, HostPeer: 1, MaxPlayers: 4, ModVersion: "0.4.0",
			peers: make(map[string]peer), bannedIPs: make(map[string]time.Time),
			usedPeerIDs: map[uint16]bool{1: true},
		}},
		endpoints: make(map[string]udpSession),
	}

	join := func(name, address string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+id+"/join",
			strings.NewReader(`{"playerName":"`+name+`","modVersion":"0.4.0"}`))
		req.RemoteAddr = address
		res := httptest.NewRecorder()
		s.handleLobby(res, req)
		return res
	}

	if res := join("Alice", "203.0.113.1:5000"); res.Code != http.StatusOK {
		t.Fatalf("first join status = %d, want %d", res.Code, http.StatusOK)
	}
	if res := join("alice", "203.0.113.2:5001"); res.Code != http.StatusConflict {
		t.Fatalf("duplicate name status = %d, want %d", res.Code, http.StatusConflict)
	}

	ban := httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+id+"/ban",
		strings.NewReader(`{"playerName":"Alice"}`))
	ban.Header.Set("Authorization", "Bearer "+hostKey)
	banRes := httptest.NewRecorder()
	s.handleLobby(banRes, ban)
	if banRes.Code != http.StatusOK {
		t.Fatalf("ban status = %d, want %d", banRes.Code, http.StatusOK)
	}
	if res := join("AnotherName", "203.0.113.1:6000"); res.Code != http.StatusForbidden {
		t.Fatalf("banned IP status = %d, want %d", res.Code, http.StatusForbidden)
	}
	if res := join("AnotherName", "203.0.113.3:6001"); res.Code != http.StatusOK {
		t.Fatalf("different IP status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestJoinUsesLegacyVersionForMissingModVersion(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	s := &store{
		lobbies: map[string]*lobby{id: {
			ID: id, HostName: "Host", HostPeer: 1, MaxPlayers: 4, ModVersion: "0.4.0",
			peers: make(map[string]peer), bannedIPs: make(map[string]time.Time),
			usedPeerIDs: map[uint16]bool{1: true},
		}},
		endpoints: make(map[string]udpSession),
	}

	join := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/lobbies/"+id+"/join", strings.NewReader(body))
		res := httptest.NewRecorder()
		s.handleLobby(res, req)
		return res
	}

	if res := join(`{"playerName":"OldClient"}`); res.Code != http.StatusOK {
		t.Fatalf("missing version status = %d, want %d", res.Code, http.StatusOK)
	}
	if res := join(`{"playerName":"DifferentClient","modVersion":"0.3.9"}`); res.Code != http.StatusUpgradeRequired {
		t.Fatalf("different version status = %d, want %d", res.Code, http.StatusUpgradeRequired)
	}
	if res := join(`{"playerName":"CurrentClient","modVersion":"0.4.0"}`); res.Code != http.StatusOK {
		t.Fatalf("matching version status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestCreateUsesLegacyVersionForMissingModVersion(t *testing.T) {
	s := &store{lobbies: make(map[string]*lobby), endpoints: make(map[string]udpSession)}
	req := httptest.NewRequest(http.MethodPost, "/v1/lobbies", strings.NewReader(`{
		"name":"Legacy lobby", "hostName":"Host", "map":"Map", "maxPlayers":4,
		"hostPort":7777, "respawnTime":0
	}`))
	res := httptest.NewRecorder()
	s.handleLobbies(res, req)

	if res.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", res.Code, http.StatusCreated)
	}
	if len(s.lobbies) != 1 {
		t.Fatalf("lobbies = %d, want 1", len(s.lobbies))
	}
	for _, l := range s.lobbies {
		if l.ModVersion != legacyModVersion {
			t.Fatalf("mod version = %q, want %q", l.ModVersion, legacyModVersion)
		}
		if !l.PlayerCollisions {
			t.Fatal("player collisions should default to enabled")
		}
		if l.Cheats {
			t.Fatal("cheats should default to disabled")
		}
	}
}

func TestCreateStoresGameRules(t *testing.T) {
	s := &store{lobbies: make(map[string]*lobby), endpoints: make(map[string]udpSession)}
	req := httptest.NewRequest(http.MethodPost, "/v1/lobbies", strings.NewReader(`{
		"name":"Rules", "hostName":"Host", "map":"Map", "maxPlayers":4,
		"hostPort":7777, "respawnTime":0, "playerCollisions":false, "cheats":true
	}`))
	res := httptest.NewRecorder()
	s.handleLobbies(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", res.Code, http.StatusCreated)
	}
	for _, l := range s.lobbies {
		if l.PlayerCollisions {
			t.Fatal("player collisions = true, want false")
		}
		if !l.Cheats {
			t.Fatal("cheats = false, want true")
		}
	}
}

func TestUDPRelayAuthenticatesAndRoutes(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	const hostKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const clientKey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	s := &store{
		lobbies: map[string]*lobby{id: {
			ID: id, HostKey: hostKey, HostPeer: 1,
			peers: map[string]peer{clientKey: {key: clientKey, peerID: 2}},
		}},
		endpoints: make(map[string]udpSession),
	}
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go serveUDP(s, server)

	host := dialUDP(t, server.LocalAddr().(*net.UDPAddr))
	defer host.Close()
	client := dialUDP(t, server.LocalAddr().(*net.UDPAddr))
	defer client.Close()

	authUDP(t, host, id, hostKey, 1)
	authUDP(t, client, id, clientKey, 2)

	fragmentPayload := []byte{1, 0, 0, 0, 0, 0, 1, 0, 5, 0, 0, 0, 9, 8, 7, 6, 5}
	packet := make([]byte, 7+len(fragmentPayload))
	copy(packet[:4], udpMagic)
	packet[4] = udpData
	packet[5] = 1 // target host
	packet[6] = 0
	copy(packet[7:], fragmentPayload)
	if _, err := client.Write(packet); err != nil {
		t.Fatal(err)
	}

	_ = host.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 128)
	n, err := host.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	got := buffer[:n]
	if len(got) != 7+len(fragmentPayload) || got[4] != udpForwarded || got[5] != 2 || got[6] != 0 {
		t.Fatalf("unexpected forwarded packet: %v", got)
	}
	for i := range fragmentPayload {
		if got[7+i] != fragmentPayload[i] {
			t.Fatalf("payload mismatch: %v", got)
		}
	}
}

func dialUDP(t *testing.T, address *net.UDPAddr) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func authUDP(t *testing.T, conn *net.UDPConn, id, key string, expectedPeer uint16) {
	t.Helper()
	packet := make([]byte, 5+64)
	copy(packet[:4], udpMagic)
	packet[4] = udpAuth
	copy(packet[5:37], []byte(id))
	copy(packet[37:69], []byte(key))
	if _, err := conn.Write(packet); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	response := make([]byte, 32)
	n, err := conn.Read(response)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 || response[4] != udpAuthOK || uint16(response[5])|uint16(response[6])<<8 != expectedPeer {
		t.Fatalf("unexpected auth response: %v", response[:n])
	}
}
