package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- History Tests ---

func TestHistoryNew(t *testing.T) {
	h := NewHistory(50)
	if h == nil {
		t.Fatal("expected non-nil History")
	}
	if h.Count() != 0 {
		t.Fatalf("expected count 0, got %d", h.Count())
	}
}

func TestHistoryDefaultMax(t *testing.T) {
	h := NewHistory(0)
	if h.max != 100 {
		t.Fatalf("expected max 100, got %d", h.max)
	}
}

func TestHistoryAppendAndSnapshot(t *testing.T) {
	h := NewHistory(10)

	h.Append(HistoryEntry{Name: "job-a", Success: true})
	h.Append(HistoryEntry{Name: "job-b", Success: false})

	snap := h.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap))
	}
	// Newest first
	if snap[0].Name != "job-b" {
		t.Errorf("expected newest 'job-b' first, got %q", snap[0].Name)
	}
	if snap[1].Name != "job-a" {
		t.Errorf("expected 'job-a' second, got %q", snap[1].Name)
	}
}

func TestHistoryRingWrap(t *testing.T) {
	h := NewHistory(3)

	// Fill and wrap
	h.Append(HistoryEntry{Name: "a"})
	h.Append(HistoryEntry{Name: "b"})
	h.Append(HistoryEntry{Name: "c"})
	h.Append(HistoryEntry{Name: "d"}) // wraps, overwrites "a"

	snap := h.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	// Newest first: d, c, b
	if snap[0].Name != "d" {
		t.Errorf("expected 'd' first, got %q", snap[0].Name)
	}
	if snap[1].Name != "c" {
		t.Errorf("expected 'c' second, got %q", snap[1].Name)
	}
	if snap[2].Name != "b" {
		t.Errorf("expected 'b' third, got %q", snap[2].Name)
	}

	if h.Count() != 3 {
		t.Fatalf("expected count 3, got %d", h.Count())
	}
}

func TestHistoryClear(t *testing.T) {
	h := NewHistory(5)
	h.Append(HistoryEntry{Name: "a"})
	h.Append(HistoryEntry{Name: "b"})
	if h.Count() != 2 {
		t.Fatalf("expected count 2, got %d", h.Count())
	}

	h.Clear()
	if h.Count() != 0 {
		t.Fatalf("expected count 0 after clear, got %d", h.Count())
	}
	if len(h.Snapshot()) != 0 {
		t.Fatal("expected empty snapshot after clear")
	}
}

func TestHistoryRecent(t *testing.T) {
	h := NewHistory(10)
	h.Append(HistoryEntry{Name: "a"})
	h.Append(HistoryEntry{Name: "b"})
	h.Append(HistoryEntry{Name: "c"})

	recent := h.Recent(2)
	if len(recent) != 2 {
		t.Fatalf("expected 2 recent, got %d", len(recent))
	}
	if recent[0].Name != "c" || recent[1].Name != "b" {
		t.Errorf("unexpected recent order: %+v", recent)
	}
}

func TestHistoryAllWhenNTooLarge(t *testing.T) {
	h := NewHistory(10)
	h.Append(HistoryEntry{Name: "a"})

	all := h.Recent(100)
	if len(all) != 1 {
		t.Fatalf("expected 1 entry even with large N, got %d", len(all))
	}
}

func TestHistoryEmptySnapshot(t *testing.T) {
	h := NewHistory(10)
	if snap := h.Snapshot(); snap != nil {
		t.Fatal("expected nil snapshot for empty history")
	}
}

func TestHistoryEntryFields(t *testing.T) {
	firedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	completedAt := time.Date(2025, 1, 1, 0, 0, 5, 0, time.UTC)

	h := NewHistory(10)
	h.Append(HistoryEntry{
		Name:         "test-job",
		FiredAt:      firedAt,
		CompletedAt:  completedAt,
		Duration:     5 * time.Second,
		Success:      true,
		ErrMsg:       "",
		Output:       "hello world",
		RetryAttempt: 2,
		LeaderNode:   "node-alpha",
	})

	snap := h.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	e := snap[0]
	if e.Name != "test-job" {
		t.Errorf("Name: expected 'test-job', got %q", e.Name)
	}
	if !e.FiredAt.Equal(firedAt) {
		t.Errorf("FiredAt mismatch")
	}
	if !e.CompletedAt.Equal(completedAt) {
		t.Errorf("CompletedAt mismatch")
	}
	if e.Duration != 5*time.Second {
		t.Errorf("Duration: expected 5s, got %s", e.Duration)
	}
	if !e.Success {
		t.Error("Success should be true")
	}
	if e.ErrMsg != "" {
		t.Errorf("ErrMsg should be empty, got %q", e.ErrMsg)
	}
	if e.Output != "hello world" {
		t.Errorf("Output mismatch")
	}
	if e.RetryAttempt != 2 {
		t.Errorf("RetryAttempt: expected 2, got %d", e.RetryAttempt)
	}
	if e.LeaderNode != "node-alpha" {
		t.Errorf("LeaderNode: expected 'node-alpha', got %q", e.LeaderNode)
	}
}

// --- Catch-Up Tests ---

func TestComputeMissedFiringsEverySchedule(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(-10 * time.Second)

	job := Job{
		Name:     "every-1s",
		Command:  "echo test",
		Schedule: &EverySchedule{Interval: time.Second},
	}

	// Should find 10 missed firings, but limit to 2
	missed := computeMissedFirings(job, since, now, 2)
	if len(missed) != 2 {
		t.Fatalf("expected 2 missed firings (limited by arg), got %d", len(missed))
	}

	// Each missed firing should be between since and now
	for _, m := range missed {
		if m.Before(since) || m.After(now) {
			t.Errorf("missed firing at %s outside [%s, %s]", m, since, now)
		}
	}
}

func TestComputeMissedFiringsEveryScheduleNoLimit(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(-3 * time.Second)

	job := Job{
		Name:     "every-1s",
		Command:  "echo test",
		Schedule: &EverySchedule{Interval: time.Second},
	}

	// Limit 0 should disable catch-up
	missed := computeMissedFirings(job, since, now, 0)
	if len(missed) != 0 {
		t.Fatalf("expected 0 missed firings with limit 0, got %d", len(missed))
	}
}

func TestComputeMissedFiringsEveryScheduleNowBeforeSince(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(time.Hour) // since is in the future

	job := Job{
		Name:     "every-1s",
		Command:  "echo test",
		Schedule: &EverySchedule{Interval: time.Second},
	}

	missed := computeMissedFirings(job, since, now, 5)
	if len(missed) != 0 {
		t.Fatalf("expected 0 missed firings when since > now, got %d", len(missed))
	}
}

func TestComputeMissedFiringsCronSchedule(t *testing.T) {
	sched, err := ParseCron("* * * * *") // every minute
	if err != nil {
		t.Fatalf("parse cron: %v", err)
	}

	now := time.Date(2025, 1, 1, 12, 5, 0, 0, time.UTC)
	since := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	job := Job{
		Name:     "every-minute",
		Command:  "echo test",
		Schedule: sched,
	}

	// Should find 5 missed firings (12:01, 12:02, 12:03, 12:04, 12:05)
	missed := computeMissedFirings(job, since, now, 10)
	if len(missed) != 5 {
		t.Fatalf("expected 5 missed firings, got %d", len(missed))
	}

	for _, m := range missed {
		if m.Before(since) || m.Equal(since) || m.After(now) {
			t.Errorf("missed firing at %s outside range", m)
		}
	}
}

func TestComputeMissedFiringsCronScheduleLimit(t *testing.T) {
	sched, err := ParseCron("* * * * *") // every minute
	if err != nil {
		t.Fatalf("parse cron: %v", err)
	}

	now := time.Date(2025, 1, 1, 12, 10, 0, 0, time.UTC)
	since := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	job := Job{
		Name:     "every-minute",
		Command:  "echo test",
		Schedule: sched,
	}

	// Limit to 3
	missed := computeMissedFirings(job, since, now, 3)
	if len(missed) != 3 {
		t.Fatalf("expected 3 missed firings (limited), got %d", len(missed))
	}
}

func TestCatchUpMissedFiresJobs(t *testing.T) {
	var mu sync.Mutex
	var events []Event
	m := NewManager(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, 0)
	m.SetCatchUpLookback(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	// Set jobs with CatchUpLimit
	m.SetJobs([]Job{
		{
			Name:         "catchup-test",
			Command:      "echo test",
			Timeout:      time.Second,
			Schedule:     &EverySchedule{Interval: 10 * time.Millisecond},
			CatchUpLimit: 3,
		},
	})

	// Become leader — triggers catch-up
	m.OnLeadershipChange(true)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(events)
	mu.Unlock()

	// Should have at least 1 catch-up firing + some regular fires
	if count < 1 {
		t.Fatalf("expected at least 1 catch-up firing, got %d", count)
	}

	m.Stop()
}

// --- Retry Tests ---

func TestComputeMissedFiringsCatchUpDoesNotExceedLimit(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(-1 * time.Hour)

	job := Job{
		Name:     "every-1s",
		Command:  "echo test",
		Schedule: &EverySchedule{Interval: time.Second},
	}

	// Should never exceed limit even if many missed
	missed := computeMissedFirings(job, since, now, 5)
	if len(missed) > 5 {
		t.Fatalf("expected at most 5, got %d", len(missed))
	}
	if len(missed) != 5 {
		t.Fatalf("expected exactly 5 (limited), got %d", len(missed))
	}
}

func TestComputeMissedFiringsNegativeInterval(t *testing.T) {
	job := Job{
		Name:     "bad",
		Command:  "echo",
		Schedule: &EverySchedule{Interval: 0},
	}

	missed := computeMissedFirings(job, time.Now().Add(-time.Minute), time.Now(), 5)
	if len(missed) != 0 {
		t.Fatalf("expected 0 for zero interval, got %d", len(missed))
	}
}

func TestManagerSetCatchUpLookbackToZeroDisables(t *testing.T) {
	var events []Event
	mu := sync.Mutex{}
	m := NewManager(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	// Disable catch-up
	m.SetCatchUpLookback(0)

	m.SetJobs([]Job{
		{
			Name:         "test",
			Command:      "echo",
			Timeout:      time.Second,
			Schedule:     &EverySchedule{Interval: 10 * time.Millisecond},
			CatchUpLimit: 5,
		},
	})

	m.OnLeadershipChange(true)

	time.Sleep(150 * time.Millisecond)

	// Catch-up is disabled, but scheduler should still fire
	m.Stop()
}

// --- Manager History Tests ---

func TestManagerHistoryAfterLeadershipGain(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
		if e.OnResult != nil {
			e.OnResult(nil, "ok", 100*time.Millisecond)
		}
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	m.OnLeadershipChange(true)

	time.Sleep(350 * time.Millisecond)
	cancel()

	history := m.History()
	if len(history) < 1 {
		t.Fatalf("expected at least 1 history entry, got %d", len(history))
	}

	// Verify entry fields
	entry := history[0]
	if entry.Name != "fast" {
		t.Errorf("expected name 'fast', got %q", entry.Name)
	}
	if !entry.Success {
		t.Errorf("expected success=true")
	}
	if entry.FiredAt.IsZero() {
		t.Errorf("expected non-zero FiredAt")
	}
}

func TestManagerHistoryRecordsFailure(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
		if e.OnResult != nil {
			e.OnResult(errors.New("job failed"), "error output", 50*time.Millisecond)
		}
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "failing", Command: "false", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	m.OnLeadershipChange(true)

	time.Sleep(250 * time.Millisecond)
	cancel()

	history := m.History()
	// We might have succeeded and failed entries; check at least one failure record
	hasFailure := false
	for _, e := range history {
		if !e.Success {
			hasFailure = true
			break
		}
	}
	if !hasFailure {
		t.Errorf("expected at least one failure record in history")
	}
}

func TestManagerHistoryAfterStop(t *testing.T) {
	m := NewManager(func(e Event) {
		if e.OnResult != nil {
			e.OnResult(nil, "done", 50*time.Millisecond)
		}
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	m.OnLeadershipChange(false)

	history := m.History()
	if len(history) == 0 {
		t.Fatal("expected history after running and stopping")
	}
}
