package mesh

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blaspat/flare/internal/election"
)

// E2E test: two nodes discover each other, exchange heartbeats,
// relay messages, and elect a leader (lowest-name wins).

func TestE2E_TwoNodeMesh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Node A (alpha, :19721) ---
	hubA := NewHub(func(p *PeerState) { _ = p })
	var onPeerChangeA atomic.Int64
	hubA.OnPeerChange(func() { onPeerChangeA.Add(1) })

	_ = StartListener(ctx, "127.0.0.1:19721", "alpha", hubA, "", "", nil)

	// --- Node B (beta, :19722) ---
	hubB := NewHub(func(p *PeerState) { _ = p })
	var onPeerChangeB atomic.Int64
	hubB.OnPeerChange(func() { onPeerChangeB.Add(1) })

	_ = StartListener(ctx, "127.0.0.1:19722", "beta", hubB, "", "", nil)

	// Let listeners start
	time.Sleep(200 * time.Millisecond)

	// --- Beta connects to Alpha ---
	_, err := Connect(ctx, "ws://127.0.0.1:19721/mesh", "beta", hubB, nil)
	if err != nil {
		t.Fatalf("beta connect to alpha: %v", err)
	}

	// Wait for connection, hello handshake, and first heartbeat
	time.Sleep(2 * time.Second)

	// Alpha should see 1 peer (beta)
	if count := hubA.Count(); count != 1 {
		t.Errorf("alpha peers: want 1, got %d", count)
	}
	// Beta should see 1 peer (alpha)
	if count := hubB.Count(); count != 1 {
		t.Errorf("beta peers: want 1, got %d", count)
	}

	// Verify peer names
	peersA := hubA.List()
	if len(peersA) != 1 || peersA[0].Name != "beta" {
		t.Errorf("alpha peer list: want [beta], got %v", peerNames(peersA))
	}
	peersB := hubB.List()
	if len(peersB) != 1 || peersB[0].Name != "alpha" {
		t.Errorf("beta peer list: want [alpha], got %v", peerNames(peersB))
	}

	// Verify peer change callbacks fired on both sides
	if n := onPeerChangeA.Load(); n < 1 {
		t.Errorf("alpha peer-change callbacks: want >=1, got %d", n)
	}
	if n := onPeerChangeB.Load(); n < 1 {
		t.Errorf("beta peer-change callbacks: want >=1, got %d", n)
	}

	// Verify both peers are alive
	for _, p := range hubA.List() {
		if !p.IsAlive() {
			t.Errorf("alpha: peer %s is not alive", p.Name)
		}
	}
	for _, p := range hubB.List() {
		if !p.IsAlive() {
			t.Errorf("beta: peer %s is not alive", p.Name)
		}
	}
}

func TestE2E_TwoNodeMessageRelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Node A (alpha, :19723) ---
	hubA := NewHub(func(p *PeerState) { _ = p })
	_ = StartListener(ctx, "127.0.0.1:19723", "alpha", hubA, "", "", nil)

	// --- Node B (beta, :19724) ---
	hubB := NewHub(func(p *PeerState) { _ = p })
	_ = StartListener(ctx, "127.0.0.1:19724", "beta", hubB, "", "", nil)

	time.Sleep(200 * time.Millisecond)

	// --- Connect beta -> alpha ---
	_, err := Connect(ctx, "ws://127.0.0.1:19723/mesh", "beta", hubB, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	time.Sleep(1 * time.Second)

	// --- Register a custom message handler on both hubs for "test" messages ---
	receivedA := make(chan *Message, 1)
	hubA.HandleMessageType("test", func(msg *Message, peer *PeerState) {
		receivedA <- msg
	})

	receivedB := make(chan *Message, 1)
	hubB.HandleMessageType("test", func(msg *Message, peer *PeerState) {
		receivedB <- msg
	})

	// --- Alpha broadcasts a test message ---
	testPayload := struct {
		Text string `json:"text"`
	}{Text: "hello from alpha"}

	msg, err := NewMessage("test", "alpha", testPayload)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	hubA.Broadcast(data)

	// Beta should receive it
	select {
	case received := <-receivedB:
		if received.From != "alpha" {
			t.Errorf("message from: want alpha, got %s", received.From)
		}
		if received.Type != "test" {
			t.Errorf("message type: want test, got %s", received.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message on beta")
	}

	// --- Beta broadcasts back ---
	msg2, err := NewMessage("test", "beta", testPayload)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	data2, err := msg2.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	hubB.Broadcast(data2)

	select {
	case received := <-receivedA:
		if received.From != "beta" {
			t.Errorf("message from: want beta, got %s", received.From)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message on alpha")
	}

	// Only the expected messages — no extra messages leaked
	select {
	case <-receivedA:
		t.Error("unexpected extra message on alpha")
	case <-receivedB:
		t.Error("unexpected extra message on beta")
	case <-time.After(500 * time.Millisecond):
		// ok — no extras
	}
}

// TestE2E_TwoNodeLeadership tests that the lowest-name node is elected leader
// when two nodes form a mesh.
func TestE2E_TwoNodeLeadership(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Node A (alpha, :19725) — lower name = expected leader ---
	hubA := NewHub(func(p *PeerState) { _ = p })
	eA := election.NewElector("alpha", func(isLeader bool) { _ = isLeader })
	hubA.OnPeerChange(func() { eA.Elect(hubA.ListNames()) })
	eA.Elect(hubA.ListNames())
	_ = StartListener(ctx, "127.0.0.1:19725", "alpha", hubA, "", "", nil)

	// --- Node B (beta, :19726) ---
	hubB := NewHub(func(p *PeerState) { _ = p })
	eB := election.NewElector("beta", func(isLeader bool) { _ = isLeader })
	hubB.OnPeerChange(func() { eB.Elect(hubB.ListNames()) })
	eB.Elect(hubB.ListNames())
	_ = StartListener(ctx, "127.0.0.1:19726", "beta", hubB, "", "", nil)

	time.Sleep(200 * time.Millisecond)

	// Connect beta -> alpha
	_, err := Connect(ctx, "ws://127.0.0.1:19725/mesh", "beta", hubB, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Wait for connection and peer change callback
	time.Sleep(2 * time.Second)

	// Re-elect on both sides to reflect the updated peer set
	eA.Elect(hubA.ListNames())
	eB.Elect(hubB.ListNames())

	// Leader should be alpha (lowest name in lexicographic order)
	if leader := eA.Leader(); leader != "alpha" {
		t.Errorf("alpha elector leader: want alpha, got %s", leader)
	}
	if leader := eB.Leader(); leader != "alpha" {
		t.Errorf("beta elector leader: want alpha, got %s", leader)
	}

	if !eA.IsLeader() {
		t.Error("alpha should be leader (lowest name)")
	}
	if eB.IsLeader() {
		t.Error("beta should NOT be leader")
	}

	// Members should contain both nodes
	membersA := eA.Members()
	if len(membersA) != 2 {
		t.Errorf("alpha members: want 2, got %d: %v", len(membersA), membersA)
	}
	membersB := eB.Members()
	if len(membersB) != 2 {
		t.Errorf("beta members: want 2, got %d: %v", len(membersB), membersB)
	}

	// Verify both members are present
	hasAlpha, hasBeta := false, false
	for _, m := range membersA {
		if m == "alpha" {
			hasAlpha = true
		}
		if m == "beta" {
			hasBeta = true
		}
	}
	if !hasAlpha || !hasBeta {
		t.Errorf("alpha members missing alpha=%v beta=%v", hasAlpha, hasBeta)
	}
}

// peerNames extracts names from a peer list for readable assertions.
func peerNames(peers []*PeerState) []string {
	names := make([]string, len(peers))
	for i, p := range peers {
		names[i] = p.Name
	}
	return names
}
