package election

import (
	"sort"
	"sync/atomic"
	"testing"
)

func TestNewElectorSelfIsLeader(t *testing.T) {
	e := NewElector("alpha", nil)
	if !e.IsLeader() {
		t.Error("expected self to be leader with no peers")
	}
	if e.Leader() != "alpha" {
		t.Errorf("expected leader 'alpha', got %q", e.Leader())
	}
}

func TestElectorMembers(t *testing.T) {
	e := NewElector("alpha", nil)
	mems := e.Members()
	if len(mems) != 1 || mems[0] != "alpha" {
		t.Fatalf("expected [alpha], got %v", mems)
	}
}

func TestElectLowerNameWins(t *testing.T) {
	e := NewElector("omega", nil)
	// omega has peers alpha, beta — alpha is lowest
	e.Elect([]string{"alpha", "beta"})

	if e.Leader() != "alpha" {
		t.Errorf("expected leader 'alpha', got %q", e.Leader())
	}
	if e.IsLeader() {
		t.Error("omega should not be leader when alpha is present")
	}
}

func TestElectSelfIsLowest(t *testing.T) {
	e := NewElector("alpha", nil)
	e.Elect([]string{"beta", "gamma"})

	if !e.IsLeader() {
		t.Error("expected alpha to be leader (lowest name)")
	}
	if e.Leader() != "alpha" {
		t.Errorf("expected leader 'alpha', got %q", e.Leader())
	}
}

func TestElectNoPeers(t *testing.T) {
	e := NewElector("alpha", nil)
	e.Elect(nil)

	if !e.IsLeader() {
		t.Error("expected alpha to be leader with nil peers")
	}
}

func TestElectEmptyPeers(t *testing.T) {
	e := NewElector("alpha", nil)
	e.Elect([]string{})

	if !e.IsLeader() {
		t.Error("expected alpha to be leader with empty peers")
	}
}

func TestElectDuplicateNames(t *testing.T) {
	e := NewElector("alpha", nil)
	e.Elect([]string{"beta", "beta", "beta"})

	mems := e.Members()
	if len(mems) != 2 {
		t.Fatalf("expected 2 members (deduped), got %v", mems)
	}
	sort.Strings(mems)
	if mems[0] != "alpha" || mems[1] != "beta" {
		t.Errorf("expected [alpha beta], got %v", mems)
	}
}

func TestElectPeerRemoval(t *testing.T) {
	e := NewElector("alpha", nil)

	// With 3 nodes, alpha is leader
	e.Elect([]string{"beta", "gamma"})
	if !e.IsLeader() {
		t.Error("expected alpha to be leader")
	}

	// gamma goes away — alpha stays leader
	e.Elect([]string{"beta"})
	if !e.IsLeader() {
		t.Error("expected alpha to remain leader")
	}

	// everybody goes away — alpha stays leader
	e.Elect([]string{})
	if !e.IsLeader() {
		t.Error("expected alpha to be leader when alone")
	}
}

func TestElectPeerJoinLeadershipLost(t *testing.T) {
	// alpha is leader alone, then ajax joins with lower name
	e := NewElector("alpha", nil)
	if !e.IsLeader() {
		t.Error("expected alpha to be leader alone")
	}

	e.Elect([]string{"ajax"})
	if e.IsLeader() {
		t.Error("alpha should lose leadership when ajax (lower name) joins")
	}
	if e.Leader() != "ajax" {
		t.Errorf("expected leader 'ajax', got %q", e.Leader())
	}
}

func TestElectCallbackOnChange(t *testing.T) {
	var calls atomic.Int32
	var lastIsLeader atomic.Bool

	e := NewElector("alpha", func(isLeader bool) {
		calls.Add(1)
		if isLeader {
			lastIsLeader.Store(true)
		} else {
			lastIsLeader.Store(false)
		}
	})

	// Initial state: no callback on creation
	if calls.Load() != 0 {
		t.Fatal("expected 0 callback calls on creation")
	}

	// First Elect with peers — alpha loses to ajax
	e.Elect([]string{"ajax"})
	if calls.Load() != 1 {
		t.Fatalf("expected 1 callback call, got %d", calls.Load())
	}
	if lastIsLeader.Load() {
		t.Error("expected isLeader=false callback")
	}

	// Elect again with same peers — no change, no callback
	e.Elect([]string{"ajax"})
	if calls.Load() != 1 {
		t.Fatalf("expected no extra callback when no change, got %d", calls.Load())
	}

	// ajax leaves — alpha becomes leader again
	e.Elect([]string{})
	if calls.Load() != 2 {
		t.Fatalf("expected 2 callback calls, got %d", calls.Load())
	}
	if !lastIsLeader.Load() {
		t.Error("expected isLeader=true callback")
	}
}

func TestElectCallbackOnFirstElectOnly(t *testing.T) {
	// alpha alone, unchanged callback should not fire when Elect is called
	// with same set
	var calls atomic.Int32
	e := NewElector("alpha", func(isLeader bool) {
		calls.Add(1)
	})

	// Elect with no peers (same as initial state)
	e.Elect([]string{})
	if calls.Load() != 0 {
		t.Fatalf("expected 0 callbacks when state unchanged, got %d", calls.Load())
	}
}

func TestElectMembersSorted(t *testing.T) {
	e := NewElector("delta", nil)
	e.Elect([]string{"beta", "charlie", "alpha"})

	mems := e.Members()
	expected := []string{"alpha", "beta", "charlie", "delta"}
	for i, m := range mems {
		if m != expected[i] {
			t.Fatalf("expected %v, got %v", expected, mems)
		}
	}
}

func TestElectSelfInMembers(t *testing.T) {
	e := NewElector("me", nil)
	e.Elect([]string{"peer-a", "peer-b"})

	mems := e.Members()
	found := false
	for _, m := range mems {
		if m == "me" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected self 'me' in members, got %v", mems)
	}
}

func TestElectorGettersAfterElect(t *testing.T) {
	e := NewElector("beta", nil)
	e.Elect([]string{"alpha", "gamma"})

	if l := e.Leader(); l != "alpha" {
		t.Errorf("Leader() = %q, want 'alpha'", l)
	}
	if i := e.IsLeader(); i {
		t.Error("IsLeader() = true, want false")
	}
}
