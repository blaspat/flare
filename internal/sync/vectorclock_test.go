package sync

import (
	"encoding/json"
	"testing"
)

func TestNewVectorClock_Empty(t *testing.T) {
	vc := NewVectorClock()
	if vc == nil {
		t.Fatal("NewVectorClock returned nil")
	}
	if len(vc.All()) != 0 {
		t.Errorf("expected empty clock, got %v", vc.All())
	}
}

func TestNewVectorClockWith(t *testing.T) {
	vc := NewVectorClockWith("node-a", 1)
	if vc.Get("node-a") != 1 {
		t.Errorf("want counter=1, got %d", vc.Get("node-a"))
	}
	if vc.Get("node-b") != 0 {
		t.Errorf("want counter=0 for unknown node, got %d", vc.Get("node-b"))
	}
}

func TestIncrement(t *testing.T) {
	vc := NewVectorClock()
	v := vc.Increment("node-a")
	if v != 1 {
		t.Errorf("first increment: want 1, got %d", v)
	}
	v = vc.Increment("node-a")
	if v != 2 {
		t.Errorf("second increment: want 2, got %d", v)
	}
	v = vc.Increment("node-b")
	if v != 1 {
		t.Errorf("increment different node: want 1, got %d", v)
	}
	if vc.Get("node-a") != 2 {
		t.Errorf("node-a should still be 2, got %d", vc.Get("node-a"))
	}
}

func TestSet(t *testing.T) {
	vc := NewVectorClock()
	vc.Set("node-a", 5)
	if vc.Get("node-a") != 5 {
		t.Errorf("want 5, got %d", vc.Get("node-a"))
	}
	vc.Set("node-a", 10)
	if vc.Get("node-a") != 10 {
		t.Errorf("want 10, got %d", vc.Get("node-a"))
	}
}

func TestMerge(t *testing.T) {
	a := NewVectorClock()
	a.Increment("node-a")
	a.Increment("node-a")
	a.Increment("node-b")

	b := NewVectorClock()
	b.Increment("node-b")
	b.Increment("node-b")
	b.Set("node-c", 1)

	a.Merge(b)

	if a.Get("node-a") != 2 {
		t.Errorf("node-a: want 2, got %d", a.Get("node-a"))
	}
	if a.Get("node-b") != 2 {
		t.Errorf("node-b: want 2, got %d", a.Get("node-b"))
	}
	if a.Get("node-c") != 1 {
		t.Errorf("node-c: want 1, got %d", a.Get("node-c"))
	}
}

func TestMerge_Idempotent(t *testing.T) {
	a := NewVectorClockWith("node-a", 1)
	b := NewVectorClockWith("node-a", 1)

	a.Merge(b)
	if a.Get("node-a") != 1 {
		t.Errorf("idempotent merge: want 1, got %d", a.Get("node-a"))
	}
}

func TestCompare_HappenedBefore(t *testing.T) {
	a := NewVectorClockWith("node-a", 1)
	b := NewVectorClockWith("node-a", 2)

	if rel := a.Compare(b); rel != HappenedBefore {
		t.Errorf("a={a:1} vs b={a:2}: want HappenedBefore(-1), got %d", rel)
	}

	// a is strictly less across all entries
	c := NewVectorClock()
	c.Set("node-a", 1)
	c.Set("node-b", 2)

	d := NewVectorClock()
	d.Set("node-a", 2)
	d.Set("node-b", 3)

	if rel := c.Compare(d); rel != HappenedBefore {
		t.Errorf("c={a:1,b:2} vs d={a:2,b:3}: want HappenedBefore(-1), got %d", rel)
	}
}

func TestCompare_HappenedAfter(t *testing.T) {
	a := NewVectorClockWith("node-a", 3)
	b := NewVectorClockWith("node-a", 1)

	if rel := a.Compare(b); rel != HappenedAfter {
		t.Errorf("a={a:3} vs b={a:1}: want HappenedAfter(1), got %d", rel)
	}
}

func TestCompare_Concurrent(t *testing.T) {
	// Different nodes — neither dominates
	a := NewVectorClockWith("node-a", 1)
	b := NewVectorClockWith("node-b", 1)

	if rel := a.Compare(b); rel != Concurrent {
		t.Errorf("a={a:1} vs b={b:1}: want Concurrent(0), got %d", rel)
	}

	// Conflict: a has node-a > b's, but node-b < b's
	c := NewVectorClock()
	c.Set("node-a", 2)
	c.Set("node-b", 1)

	d := NewVectorClock()
	d.Set("node-a", 1)
	d.Set("node-b", 2)

	if rel := c.Compare(d); rel != Concurrent {
		t.Errorf("conflict: want Concurrent(0), got %d", rel)
	}
}

func TestCompare_Equal(t *testing.T) {
	a := NewVectorClock()
	a.Set("node-a", 1)
	a.Set("node-b", 2)

	b := NewVectorClock()
	b.Set("node-a", 1)
	b.Set("node-b", 2)

	// Equal clocks are Concurrent (neither dominated).
	if rel := a.Compare(b); rel != Concurrent {
		t.Errorf("equal clocks: want Concurrent(0), got %d", rel)
	}
}

func TestCompare_EmptyVsNonEmpty(t *testing.T) {
	empty := NewVectorClock()
	nonEmpty := NewVectorClockWith("node-a", 1)

	// empty vs nonEmpty: empty has 0 for node-a, nonEmpty has 1
	// 0 < 1 → less=true for node-a, no greater → HappenedBefore
	if rel := empty.Compare(nonEmpty); rel != HappenedBefore {
		t.Errorf("empty vs {a:1}: want HappenedBefore(-1), got %d", rel)
	}

	// nonEmpty vs empty: 1 > 0 → greater=true, no less → HappenedAfter
	if rel := nonEmpty.Compare(empty); rel != HappenedAfter {
		t.Errorf("{a:1} vs empty: want HappenedAfter(1), got %d", rel)
	}
}

func TestCopy_IsIndependent(t *testing.T) {
	original := NewVectorClock()
	original.Increment("node-a")
	original.Increment("node-b")

	copied := original.Copy()
	copied.Increment("node-a")

	if original.Get("node-a") != 1 {
		t.Errorf("original should be unchanged at 1, got %d", original.Get("node-a"))
	}
	if copied.Get("node-a") != 2 {
		t.Errorf("copy should be 2, got %d", copied.Get("node-a"))
	}
	// original should still have node-a=1, node-b=1
	if original.Get("node-a") != 1 {
		t.Errorf("original node-a should be 1, got %d", original.Get("node-a"))
	}
	if original.Get("node-b") != 1 {
		t.Errorf("original node-b should be 1, got %d", original.Get("node-b"))
	}
}

func TestAll_ReturnsCopy(t *testing.T) {
	vc := NewVectorClock()
	vc.Set("node-a", 1)

	all := vc.All()
	all["node-a"] = 99

	if vc.Get("node-a") != 1 {
		t.Error("mutating All() result should not affect original")
	}
}

func TestEqual(t *testing.T) {
	a := NewVectorClock()
	a.Set("node-a", 1)
	a.Set("node-b", 2)

	b := NewVectorClock()
	b.Set("node-a", 1)
	b.Set("node-b", 2)

	if !a.Equal(b) {
		t.Error("identical clocks should be equal")
	}

	b.Increment("node-c")
	if a.Equal(b) {
		t.Error("different clocks should not be equal")
	}
}

func TestString(t *testing.T) {
	vc := NewVectorClock()
	if s := vc.String(); s != "{}" {
		t.Errorf("empty: want '{}', got %q", s)
	}

	vc.Set("node-b", 2)
	vc.Set("node-a", 1)

	s := vc.String()
	wantA := "{node-a: 1, node-b: 2}"
	wantB := "{node-b: 2, node-a: 1}"
	if s != wantA && s != wantB {
		t.Errorf("want %q or %q, got %q", wantA, wantB, s)
	}
}

func TestJSON_RoundTrip(t *testing.T) {
	vc := NewVectorClock()
	vc.Set("node-a", 3)
	vc.Set("node-b", 7)

	data, err := json.Marshal(vc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded VectorClock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !vc.Equal(&decoded) {
		t.Errorf("round-trip mismatch:\n  original: %s\n  decoded:  %s", vc, &decoded)
	}
}

func TestJSON_EmptyClock(t *testing.T) {
	vc := NewVectorClock()
	data, err := json.Marshal(vc)
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	if string(data) != "{}" {
		t.Errorf("empty clock: want '{}', got %s", string(data))
	}

	var decoded VectorClock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(decoded.All()) != 0 {
		t.Errorf("decoded empty should be empty, got %v", decoded.All())
	}
}

func TestJSON_NilUnmarshalProducesEmpty(t *testing.T) {
	// Ensure UnmarshalJSON initialises a nil receiver.
	var vc VectorClock
	data := []byte(`{"node-x": 42}`)
	if err := json.Unmarshal(data, &vc); err != nil {
		t.Fatalf("unmarshal into zero struct: %v", err)
	}
	if vc.Get("node-x") != 42 {
		t.Errorf("want 42, got %d", vc.Get("node-x"))
	}
}

func TestConcurrentMerge_ResolvesToLargest(t *testing.T) {
	a := NewVectorClock()
	a.Set("node-a", 1)
	a.Set("node-b", 5)

	b := NewVectorClock()
	b.Set("node-a", 3)
	b.Set("node-b", 2)

	a.Merge(b)

	if a.Get("node-a") != 3 {
		t.Errorf("node-a: want max(1,3)=3, got %d", a.Get("node-a"))
	}
	if a.Get("node-b") != 5 {
		t.Errorf("node-b: want max(5,2)=5, got %d", a.Get("node-b"))
	}
}

func TestConcurrentMerge_Reversible(t *testing.T) {
	a := NewVectorClockWith("node-a", 1)
	b := NewVectorClockWith("node-b", 1)

	a.Merge(b)
	b.Merge(a)

	if !a.Equal(b) {
		t.Errorf("reversible merge should produce equal clocks:\n  a: %s\n  b: %s", a, b)
	}
}

// --- Integration: causal ordering across three nodes -------------------

func TestThreeNodeCausality(t *testing.T) {
	// Simulate: node-a edits file (v=1), node-b receives and edits (v=2),
	// node-c receives both. Check causal ordering holds.

	a := NewVectorClock()
	b := NewVectorClock()
	c := NewVectorClock()

	// Node-a edits: a increments.
	a.Increment("node-a") // a={a:1}

	// Node-b receives a's update (merge) and then edits.
	b.Merge(a)
	b.Increment("node-b") // b={a:1, b:1}

	// Node-c receives a's update.
	c.Merge(a) // c={a:1}

	// a happened before b
	if rel := a.Compare(b); rel != HappenedBefore {
		t.Errorf("a {a:1} vs b {a:1,b:1}: want HappenedBefore, got %d", rel)
	}

	// c only has a's update, so c happened before b too
	if rel := c.Compare(b); rel != HappenedBefore {
		t.Errorf("c {a:1} vs b {a:1,b:1}: want HappenedBefore, got %d", rel)
	}

	// a and c are concurrent (different nodes' first edits, no causal link)
	// Actually, c only has a's state, so c = {a:1} and a = {a:1}. They're equal → concurrent.
	if rel := a.Compare(c); rel != Concurrent {
		t.Errorf("a {a:1} vs c {a:1}: want Concurrent (equal), got %d", rel)
	}
}

func TestCompare_ThreeWayConflict(t *testing.T) {
	// Three nodes all edit concurrently
	a := NewVectorClock()
	a.Increment("node-a") // {a:1}

	b := NewVectorClock()
	b.Increment("node-b") // {b:1}

	c := NewVectorClock()
	c.Increment("node-c") // {c:1}

	// All pairs are concurrent
	if rel := a.Compare(b); rel != Concurrent {
		t.Errorf("a vs b: want Concurrent, got %d", rel)
	}
	if rel := a.Compare(c); rel != Concurrent {
		t.Errorf("a vs c: want Concurrent, got %d", rel)
	}
	if rel := b.Compare(c); rel != Concurrent {
		t.Errorf("b vs c: want Concurrent, got %d", rel)
	}
}
