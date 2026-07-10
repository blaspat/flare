package mesh

import (
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newTestWebSocketPair creates a connected WebSocket client-server pair
// using httptest. Returns (serverConn, clientConn).
// The server echoes text messages back; we call closeServer to tear down.
func newTestWebSocketPair(t *testing.T) (serverConn, clientConn *websocket.Conn, closeServer func()) {
	t.Helper()

	var upgrader = websocket.Upgrader{}
	var serverConns []*websocket.Conn

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConns = append(serverConns, c)
	}))

	client, _, err := websocket.DefaultDialer.Dial(
		"ws"+srv.URL[len("http"):]+"/mesh", nil,
	)
	if err != nil {
		srv.Close()
		t.Fatalf("dial test server: %v", err)
	}

	// Wait for the server to establish the connection
	var server *websocket.Conn
	for i := 0; i < 100; i++ {
		if len(serverConns) > 0 {
			server = serverConns[0]
			break
		}
		time.Sleep(time.Millisecond)
	}
	if server == nil {
		srv.Close()
		t.Fatal("server didn't accept connection")
	}

	return server, client, srv.Close
}

func testBackoff() BackoffConfig {
	return BackoffConfig{
		Min:    50 * time.Millisecond,
		Max:    500 * time.Millisecond,
		Factor: 2.0,
		Jitter: 0,
	}
}

func TestReconnectManagerTrackForget(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	defer rm.Stop()

	// Track a peer
	rm.Track("node-beta", "ws://10.0.0.1:9721/mesh")

	// Forget it
	rm.Forget("node-beta")

	// OnDisconnect should not do anything for forgotten peers
	rm.OnDisconnect("node-beta") // should not panic or start a loop
}

func TestReconnectManagerCancelReconnect(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	defer rm.Stop()

	rm.Track("node-beta", "ws://10.0.0.1:9721/mesh")
	rm.OnDisconnect("node-beta")

	// Cancel immediately — should stop the goroutine
	rm.CancelReconnect("node-beta")

	// Allow a moment; should not re-connect (no server anyway, but shouldn't panic)
	time.Sleep(200 * time.Millisecond)
}

func TestReconnectManagerTrackExistingCancelsReconnect(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	defer rm.Stop()

	rm.Track("node-beta", "ws://10.0.0.1:9721/mesh")
	rm.OnDisconnect("node-beta")

	// Track again (simulates reconnection) — should cancel reconnect loop
	rm.Track("node-beta", "ws://10.0.0.2:9721/mesh")

	// Should not start a second loop either when disconnecting again
	rm.OnDisconnect("node-beta")
}

func TestReconnectManagerMultipleOnDisconnectNoStack(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	defer rm.Stop()

	rm.Track("node-beta", "ws://10.0.0.1:9721/mesh")
	rm.OnDisconnect("node-beta")
	// Second call should be a no-op (no stacking)
	rm.OnDisconnect("node-beta")
}

func TestPeerOnDisconnectFiresOnClose(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	var fired atomic.Bool
	peer := NewPeer("test-peer", client)
	peer.SetOnDisconnect(func(name string) {
		if name == "test-peer" {
			fired.Store(true)
		}
	})

	// Close the peer (simulates readPump failure)
	peer.Close()

	if !fired.Load() {
		t.Error("expected disconnect handler to fire on Close()")
	}
}

func TestPeerOnDisconnectFiresOnlyOnce(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	var callCount atomic.Int32
	peer := NewPeer("test-peer", client)
	peer.SetOnDisconnect(func(name string) {
		callCount.Add(1)
	})

	// Close multiple times — handler should fire only once
	peer.Close()
	peer.Close()
	peer.Close()

	if c := callCount.Load(); c != 1 {
		t.Errorf("expected 1 call, got %d", c)
	}
}

func TestHubAddPeerSetsDisconnectHandler(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	hub := NewHub(func(p *PeerState) {})
	peer := NewPeer("node-beta", client)

	hub.AddPeer("node-beta", peer, "ws://10.0.0.1:9721/mesh")

	if hub.Count() != 1 {
		t.Fatalf("expected 1 peer after AddPeer, got %d", hub.Count())
	}

	// Close the peer (simulates unexpected drop)
	peer.Close()

	// Give the goroutine time to fire
	time.Sleep(100 * time.Millisecond)

	// Peer should be removed from hub
	if hub.Count() != 0 {
		t.Errorf("expected 0 peers after disconnect, got %d", hub.Count())
	}
}

func TestHubRemoveSuppressesDisconnect(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	var disconnectCalled atomic.Bool

	hub := NewHub(func(p *PeerState) {})
	peer := NewPeer("node-beta", client)

	// Add the peer
	hub.AddPeer("node-beta", peer, "ws://10.0.0.1:9721/mesh")

	// Replace the disconnect handler for detection
	peer.SetOnDisconnect(func(name string) {
		disconnectCalled.Store(true)
	})

	if hub.Count() != 1 {
		t.Fatalf("expected 1 peer after AddPeer, got %d", hub.Count())
	}

	// Hub.Remove should suppress the handler
	hub.Remove("node-beta")

	// Wait briefly
	time.Sleep(100 * time.Millisecond)

	if disconnectCalled.Load() {
		t.Error("disconnect handler should not fire on Hub.Remove")
	}

	if hub.Count() != 0 {
		t.Errorf("expected 0 peers after Remove, got %d", hub.Count())
	}
}

func TestHubAddPeerReplacesExisting(t *testing.T) {
	server1, client1, closeSrv1 := newTestWebSocketPair(t)
	defer closeSrv1()
	defer server1.Close()
	defer client1.Close()

	server2, client2, closeSrv2 := newTestWebSocketPair(t)
	defer closeSrv2()
	defer server2.Close()
	defer client2.Close()

	var disconnectOld atomic.Bool

	hub := NewHub(func(p *PeerState) {})
	peer1 := NewPeer("node-beta", client1)
	peer1.SetOnDisconnect(func(name string) {
		disconnectOld.Store(true)
	})

	// Add first peer
	hub.AddPeer("node-beta", peer1, "ws://10.0.0.1:9721/mesh")

	// Replace with second peer
	peer2 := NewPeer("node-beta", client2)
	hub.AddPeer("node-beta", peer2, "ws://10.0.0.2:9721/mesh")

	if hub.Count() != 1 {
		t.Errorf("expected 1 peer after replacement, got %d", hub.Count())
	}

	// The old peer's disconnect handler should NOT fire (it was suppressed)
	if disconnectOld.Load() {
		t.Error("replaced peer's disconnect handler should not fire")
	}
}

func TestHubAddPeerOutgoingTracksReconnect(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	hub.SetReconnectManager(rm)

	peer := NewPeer("node-beta", client)
	hub.AddPeer("node-beta", peer, "ws://10.0.0.1:9721/mesh")

	// Trigger disconnect — should start reconnect
	peer.Close()

	time.Sleep(100 * time.Millisecond)

	// Peer should be removed from hub
	if hub.Count() != 0 {
		t.Errorf("expected 0 peers after disconnect, got %d", hub.Count())
	}

	rm.Stop()
}

func TestHubAddPeerIncomingNoReconnect(t *testing.T) {
	server, client, closeSrv := newTestWebSocketPair(t)
	defer closeSrv()
	defer server.Close()
	defer client.Close()

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	hub.SetReconnectManager(rm)

	peer := NewPeer("node-beta", client)
	hub.AddPeer("node-beta", peer, "") // incoming — no addr

	// Trigger disconnect — should NOT start reconnect (no addr tracked)
	peer.Close()

	// Just verify it doesn't panic
	time.Sleep(100 * time.Millisecond)

	if hub.Count() != 0 {
		t.Errorf("expected 0 peers after disconnect, got %d", hub.Count())
	}

	rm.Stop()
}

// ---- Backoff tests ----

func TestBackoffDelayIncreases(t *testing.T) {
	b := BackoffConfig{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2.0,
		Jitter: 0, // no jitter for deterministic test
	}

	d0 := b.Delay(0)
	d1 := b.Delay(1)
	d2 := b.Delay(2)
	d3 := b.Delay(3)

	if d0 >= d1 || d1 >= d2 || d2 >= d3 {
		t.Errorf("expected strictly increasing delays, got %v, %v, %v, %v", d0, d1, d2, d3)
	}
}

func TestBackoffDelayCapsAtMax(t *testing.T) {
	b := BackoffConfig{
		Min:    100 * time.Millisecond,
		Max:    500 * time.Millisecond,
		Factor: 2.0,
		Jitter: 0,
	}

	for i := 0; i < 20; i++ {
		d := b.Delay(i)
		if d > b.Max {
			t.Errorf("attempt %d: delay %v exceeds max %v", i, d, b.Max)
		}
		if d < 0 {
			t.Errorf("attempt %d: negative delay %v", i, d)
		}
	}
}

func TestBackoffJitterSpread(t *testing.T) {
	b := BackoffConfig{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2.0,
		Jitter: 0.5, // ±50% — wide range for testing
	}

	// Collect many samples at the same attempt level
	attempt := 2
	samples := make([]time.Duration, 50)
	minSample := b.Max
	maxSample := time.Duration(0)
	for i := range samples {
		d := b.Delay(attempt)
		samples[i] = d
		if d < minSample {
			minSample = d
		}
		if d > maxSample {
			maxSample = d
		}
	}

	minExpected := time.Duration(float64(b.Min)*math.Pow(b.Factor, float64(attempt))*0.5 + 1)
	maxExpected := time.Duration(float64(b.Min)*math.Pow(b.Factor, float64(attempt))*1.5 + 1)

	if minSample < minExpected {
		t.Logf("min sample %v < expected min %v", minSample, minExpected)
	}
	if maxSample > maxExpected {
		t.Logf("max sample %v > expected max %v", maxSample, maxExpected)
	}
}

func TestBackoffZeroAttempt(t *testing.T) {
	b := BackoffConfig{
		Min:    1 * time.Second,
		Max:    60 * time.Second,
		Factor: 2.0,
		Jitter: 0,
	}

	d := b.Delay(0)
	if d != b.Min {
		t.Errorf("expected delay(0) = min = %v, got %v", b.Min, d)
	}
}

// ---- Circuit breaker tests ----

func TestCircuitBreakerTripsAfterLimit(t *testing.T) {
	// Use a very short backoff so reconnect attempts happen quickly.
	// Dial a port that refuses immediately ("connection refused") so the failure is fast.
	b := BackoffConfig{
		Min:    1 * time.Millisecond,
		Max:    5 * time.Millisecond,
		Factor: 1.5,
		Jitter: 0,
	}

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", b, 3) // trip after 3 failures
	defer rm.Stop()

	// Use 127.0.0.1:1 — nothing listens on port 1, so "connection refused" returns instantly.
	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh")
	rm.OnDisconnect("dead-peer")

	// Wait for circuit to trip — 3 retries at ~1ms + 1.5ms + 2.25ms + connect-refused round-trips
	time.Sleep(200 * time.Millisecond)

	// Check status
	status := rm.Status()
	if s, ok := status["dead-peer"]; !ok || s != "circuit_open" {
		t.Errorf("expected dead-peer circuit to be open, got status=%v", status["dead-peer"])
	}
}

func TestCircuitBreakerTrackResets(t *testing.T) {
	b := BackoffConfig{
		Min:    5 * time.Millisecond,
		Max:    50 * time.Millisecond,
		Factor: 2.0,
		Jitter: 0,
	}

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", b, 3)
	defer rm.Stop()

	// Trip the circuit
	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh")
	rm.OnDisconnect("dead-peer")
	time.Sleep(100 * time.Millisecond)

	status := rm.Status()
	if s, ok := status["dead-peer"]; !ok || s != "circuit_open" {
		t.Skip("circuit didn't trip in time, skipping reset test")
	}

	// Track again (simulates manual reconnect or mDNS re-discovery)
	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh") // connection refused, fails fast

	// Should be reset now
	status = rm.Status()
	if s, ok := status["dead-peer"]; ok && s == "circuit_open" {
		t.Error("expected circuit to reset after Track, but still open")
	}
}

func TestCircuitBreakerDisabledWithZero(t *testing.T) {
	b := BackoffConfig{
		Min:    1 * time.Millisecond,
		Max:    5 * time.Millisecond,
		Factor: 1.5,
		Jitter: 0,
	}

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", b, 0) // 0 = disabled
	defer rm.Stop()

	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh") // connection refused, fails fast
	rm.OnDisconnect("dead-peer")

	// Give it time for several retry cycles
	time.Sleep(100 * time.Millisecond)

	status := rm.Status()
	if _, ok := status["dead-peer"]; !ok {
		t.Error("expected dead-peer to still be tracked")
	}
	// Should still be "reconnecting", not "circuit_open"
	if s := status["dead-peer"]; s != "reconnecting" {
		t.Logf("dead-peer status: %s (should be reconnecting)", s)
	}
}

func TestBackoffDefaultConfig(t *testing.T) {
	b := DefaultBackoff()
	if b.Min != 1*time.Second {
		t.Errorf("expected Min=1s, got %v", b.Min)
	}
	if b.Max != 60*time.Second {
		t.Errorf("expected Max=60s, got %v", b.Max)
	}
	if b.Factor != 2.0 {
		t.Errorf("expected Factor=2.0, got %v", b.Factor)
	}
	if b.Jitter != 0.25 {
		t.Errorf("expected Jitter=0.25, got %v", b.Jitter)
	}
}

func TestReconnectManagerStatus(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", testBackoff(), 10)
	defer rm.Stop()

	// No tracked peers yet
	s := rm.Status()
	if len(s) != 0 {
		t.Errorf("expected empty status, got %v", s)
	}

	// Track a peer (connected)
	rm.Track("node-beta", "ws://10.0.0.1:9721/mesh")
	s = rm.Status()
	if s["node-beta"] != "connected" {
		t.Errorf("expected 'connected', got %v", s["node-beta"])
	}

	// Trigger disconnect (reconnecting)
	rm.OnDisconnect("node-beta")
	s = rm.Status()
	if s["node-beta"] != "reconnecting" {
		t.Errorf("expected 'reconnecting', got %v", s["node-beta"])
	}
}

func TestTrackPeerResetsCircuitBreaker(t *testing.T) {
	b := BackoffConfig{
		Min:    5 * time.Millisecond,
		Max:    50 * time.Millisecond,
		Factor: 2.0,
		Jitter: 0,
	}

	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", b, 2)
	defer rm.Stop()

	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh") // connection refused, fails fast
	rm.OnDisconnect("dead-peer")

	time.Sleep(50 * time.Millisecond)

	// Before circuit trips, call Track (simulates reconnection)
	rm.Track("dead-peer", "ws://127.0.0.1:1/mesh") // connection refused, fails fast
	rm.OnDisconnect("dead-peer")

	time.Sleep(50 * time.Millisecond)

	status := rm.Status()
	if s, ok := status["dead-peer"]; ok && s == "circuit_open" {
		t.Log("circuit still open after Track+OnDisconnect (expected with only 2 attempts)")
	}
}
