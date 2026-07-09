package election

import (
	"sort"
	"sync"
)

// Elector implements simplest-first leader election.
// The leader is always the connected node with the lowest name.
// All nodes independently compute the same result given the same member set
// — no coordination messages needed.
type Elector struct {
	nodeName string
	onChange func(isLeader bool)

	mu      sync.Mutex
	members []string // sorted member names (including self)
	leader  string
}

// NewElector creates a new Elector. onChange is called when leadership
// status changes. Initially the single-member set elects this node as leader.
func NewElector(nodeName string, onChange func(isLeader bool)) *Elector {
	return &Elector{
		nodeName: nodeName,
		onChange: onChange,
		members:  []string{nodeName},
		leader:   nodeName,
	}
}

// Leader returns the current leader's node name.
func (e *Elector) Leader() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader
}

// IsLeader returns whether this node is the current leader.
func (e *Elector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader == e.nodeName
}

// Members returns a sorted copy of the current member names.
func (e *Elector) Members() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.members))
	copy(out, e.members)
	return out
}

// Elect re-evaluates leadership given the current set of connected peer
// names (excluding self). Call this whenever the peer set changes.
func (e *Elector) Elect(peerNames []string) {
	e.mu.Lock()

	// Build sorted member list (self + deduped peers).
	memberSet := make(map[string]struct{})
	memberSet[e.nodeName] = struct{}{}
	for _, name := range peerNames {
		if name != "" && name != e.nodeName {
			memberSet[name] = struct{}{}
		}
	}

	members := make([]string, 0, len(memberSet))
	for m := range memberSet {
		members = append(members, m)
	}
	sort.Strings(members)

	// Lowest name wins.
	var leader string
	for _, m := range members {
		if leader == "" || m < leader {
			leader = m
		}
	}

	changed := leader != e.leader
	isLeader := leader == e.nodeName
	e.leader = leader
	e.members = members

	e.mu.Unlock()

	if changed && e.onChange != nil {
		e.onChange(isLeader)
	}
}
