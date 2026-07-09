package mesh

import (
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

func TestReconnectManagerTrackForget(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	rm := NewReconnectManager(hub, "test-node", time.Second)
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
	rm := NewReconnectManager(hub, "test-node", 100*time.Millisecond)
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
	rm := NewReconnectManager(hub, "test-node", 100*time.Millisecond)
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
	rm := NewReconnectManager(hub, "test-node", 100*time.Millisecond)
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
	rm := NewReconnectManager(hub, "test-node", time.Second)
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
	rm := NewReconnectManager(hub, "test-node", time.Second)
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
