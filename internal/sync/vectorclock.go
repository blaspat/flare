package sync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ClockRelation describes the causal relationship between two vector clocks.
type ClockRelation int

const (
	HappenedBefore ClockRelation = -1 // a happened before b
	Concurrent     ClockRelation = 0  // a and b are concurrent (or equal)
	HappenedAfter  ClockRelation = 1  // a happened after b
)

// VectorClock tracks causal ordering of events across distributed nodes.
// Each node maintains its own counter; clocks are compared using standard
// vector-clock semantics:
//
//   - a ≤ b for all entries and a < b for at least one → HappenedBefore
//   - a ≥ b for all entries and a > b for at least one → HappenedAfter
//   - otherwise → Concurrent (includes the equal case)
//
// JSON-serializable so it can travel over the mesh wire protocol.
type VectorClock struct {
	mu     sync.RWMutex
	clocks map[string]uint64
}

// NewVectorClock creates an empty vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

// NewVectorClockWith creates a vector clock initialised with a single entry.
// Convenient when an event originates on a known node.
func NewVectorClockWith(nodeID string, counter uint64) *VectorClock {
	vc := NewVectorClock()
	vc.clocks[nodeID] = counter
	return vc
}

// Increment bumps the counter for the given node and returns the new value.
func (vc *VectorClock) Increment(nodeID string) uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID]++
	return vc.clocks[nodeID]
}

// Get returns the counter for a node (0 if the node is unknown).
func (vc *VectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[nodeID]
}

// Set sets the counter for a node to a specific value.
func (vc *VectorClock) Set(nodeID string, counter uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID] = counter
}

// Merge combines another vector clock into this one by taking the
// element-wise maximum of each node's counter. This is the standard
// merge operation for vector clocks in a gossip / peer-replicated system.
func (vc *VectorClock) Merge(other *VectorClock) {
	snapshot := other.snapshot()

	vc.mu.Lock()
	defer vc.mu.Unlock()
	for nodeID, counter := range snapshot {
		if counter > vc.clocks[nodeID] {
			vc.clocks[nodeID] = counter
		}
	}
}

// Compare returns the causal relationship between this clock and another.
// The comparison is based on standard vector-clock ordering:
//
//	vc.Compare(other) == HappenedBefore  → vc causally precedes other
//	vc.Compare(other) == Concurrent      → no causal relationship (or equal)
//	vc.Compare(other) == HappenedAfter   → vc causally succeeds other
func (vc *VectorClock) Compare(other *VectorClock) ClockRelation {
	a := vc.snapshot()
	b := other.snapshot()

	allNodes := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		allNodes[k] = struct{}{}
	}
	for k := range b {
		allNodes[k] = struct{}{}
	}

	var less, greater bool

	for nodeID := range allNodes {
		va := a[nodeID]
		vb := b[nodeID]
		switch {
		case va < vb:
			less = true
		case va > vb:
			greater = true
		}
	}

	switch {
	case less && !greater:
		return HappenedBefore
	case greater && !less:
		return HappenedAfter
	default:
		return Concurrent
	}
}

// Copy returns a deep copy of the vector clock with its own mutex.
func (vc *VectorClock) Copy() *VectorClock {
	return &VectorClock{
		clocks: vc.snapshot(),
	}
}

// All returns a copy of all clock entries.
func (vc *VectorClock) All() map[string]uint64 {
	return vc.snapshot()
}

// Equal returns true if both clocks have identical entries.
func (vc *VectorClock) Equal(other *VectorClock) bool {
	a := vc.snapshot()
	b := other.snapshot()

	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// String returns a human-readable representation, e.g. "{node-a: 3, node-b: 7}".
func (vc *VectorClock) String() string {
	m := vc.snapshot()
	if len(m) == 0 {
		return "{}"
	}
	nodes := make([]string, 0, len(m))
	for k := range m {
		nodes = append(nodes, k)
	}
	sort.Strings(nodes)

	var sb strings.Builder
	sb.WriteByte('{')
	for i, n := range nodes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s: %d", n, m[n]))
	}
	sb.WriteByte('}')
	return sb.String()
}

// FromMap creates a VectorClock from a map of node IDs to counters.
// The returned clock is independent of the input map (deep copied).
func FromMap(entries map[string]uint64) *VectorClock {
	vc := NewVectorClock()
	vc.mu.Lock()
	for k, v := range entries {
		vc.clocks[k] = v
	}
	vc.mu.Unlock()
	return vc
}

// snapshot returns a copy of the current clock entries under a read lock.
func (vc *VectorClock) snapshot() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	out := make(map[string]uint64, len(vc.clocks))
	for k, v := range vc.clocks {
		out[k] = v
	}
	return out
}

// --- JSON serialisation ------------------------------------------------

func (vc *VectorClock) MarshalJSON() ([]byte, error) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return json.Marshal(vc.clocks)
}

func (vc *VectorClock) UnmarshalJSON(data []byte) error {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	if vc.clocks == nil {
		vc.clocks = make(map[string]uint64)
	}
	return json.Unmarshal(data, &vc.clocks)
}
