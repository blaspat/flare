package cron

import (
	"sync"
	"time"
)

// HistoryEntry records a single job execution.
type HistoryEntry struct {
	// Name is the job name.
	Name string
	// FiredAt is when the job was triggered.
	FiredAt time.Time
	// CompletedAt is when the job finished (zero if still running or status unknown).
	CompletedAt time.Time
	// Duration is how long the job ran.
	Duration time.Duration
	// Success is true when the job completed without error.
	Success bool
	// ErrMsg is the error message if the job failed.
	ErrMsg string
	// Output is the last 1KB of job output (truncated).
	Output string
	// RetryAttempt is which retry this was (0 = first attempt).
	RetryAttempt int
	// LeaderNode is which node executed the job.
	LeaderNode string
}

// History is a thread-safe ring buffer of job execution records.
type History struct {
	mu     sync.Mutex
	ring   []HistoryEntry
	head   int // next write position
	count  int // number of entries currently stored
	max    int // maximum entries before wrapping
}

// NewHistory creates a history ring buffer with the given max size.
// max must be positive; values <= 0 default to 100.
func NewHistory(max int) *History {
	if max <= 0 {
		max = 100
	}
	return &History{
		ring: make([]HistoryEntry, max),
		max:  max,
	}
}

// Append adds an entry to the history ring buffer. Thread-safe.
func (h *History) Append(entry HistoryEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ring[h.head] = entry
	h.head = (h.head + 1) % h.max
	if h.count < h.max {
		h.count++
	}
}

// Snapshot returns a copy of all entries in order (newest first).
func (h *History) Snapshot() []HistoryEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count == 0 {
		return nil
	}

	result := make([]HistoryEntry, h.count)
	// Read from oldest to newest in ring order, then reverse.
	start := (h.head - h.count + h.max) % h.max
	for i := 0; i < h.count; i++ {
		result[i] = h.ring[(start+i)%h.max]
	}

	// Reverse to get newest first.
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// Recent returns the N most recent entries (newest first).
// If N <= 0 or N > count, returns all entries.
func (h *History) Recent(n int) []HistoryEntry {
	snap := h.Snapshot()
	if n <= 0 || n >= len(snap) {
		return snap
	}
	return snap[:n]
}

// Count returns the number of entries currently stored.
func (h *History) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// Clear removes all entries.
func (h *History) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.head = 0
	h.count = 0
	// Zero the ring to release references
	for i := range h.ring {
		h.ring[i] = HistoryEntry{}
	}
}
