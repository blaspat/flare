package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m := NewManager(nil, 0)
	if m == nil {
		t.Fatal("expected non-nil Manager")
	}
	if m.IsLeader() {
		t.Error("expected IsLeader()=false initially")
	}
	jobs := m.Jobs()
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(jobs))
	}
}

func TestManagerSetJobs(t *testing.T) {
	m := NewManager(nil, 0)

	jobs := []Job{
		{Name: "job-a", Command: "echo a", Timeout: time.Second, Schedule: &EverySchedule{Interval: time.Hour}},
		{Name: "job-b", Command: "echo b", Timeout: time.Second, Schedule: &EverySchedule{Interval: time.Hour}},
	}
	m.SetJobs(jobs)

	got := m.Jobs()
	if len(got) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(got))
	}
	if got[0].Name != "job-a" || got[1].Name != "job-b" {
		t.Errorf("unexpected jobs: %+v", got)
	}
}

func TestManagerOnLeadershipChangeBecomesLeader(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	// Add a job that fires every 100ms
	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Become leader — scheduler should start
	m.OnLeadershipChange(true)
	if !m.IsLeader() {
		t.Error("expected IsLeader()=true after OnLeadershipChange(true)")
	}

	// Wait for the tick to fire
	time.Sleep(300 * time.Millisecond)

	count := firedCount.Load()
	if count < 1 {
		t.Fatalf("expected at least 1 fire as leader, got %d", count)
	}
}

func TestManagerOnLeadershipChangeLoseLeadership(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Become leader, let it fire
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	before := firedCount.Load()
	if before < 1 {
		t.Fatal("expected fires before losing leadership")
	}

	// Lose leadership — scheduler should stop
	m.OnLeadershipChange(false)
	if m.IsLeader() {
		t.Error("expected IsLeader()=false after OnLeadershipChange(false)")
	}

	after := firedCount.Load()

	// Wait a bit longer to ensure no more fires
	time.Sleep(300 * time.Millisecond)

	if firedCount.Load() != after {
		t.Fatalf("scheduler fired after losing leadership: before=%d, after=%d", after, firedCount.Load())
	}
}

func TestManagerOnLeadershipChangeNoopSameValue(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Become leader
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	afterFirst := firedCount.Load()

	// Call OnLeadershipChange(true) again — should be no-op
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)

	afterSecond := firedCount.Load()
	if afterSecond <= afterFirst {
		t.Fatal("expected scheduler to keep running on duplicate OnLeadershipChange(true)")
	}
}

func TestManagerOnLeadershipChangeMultipleTransitions(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Leader -> fires
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	c1 := firedCount.Load()
	if c1 < 1 {
		t.Fatal("expected fires as leader")
	}

	// Not leader -> stops
	m.OnLeadershipChange(false)
	time.Sleep(250 * time.Millisecond)
	c2 := firedCount.Load()

	// Leader again -> fires again
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	c3 := firedCount.Load()
	if c3 <= c2 {
		t.Fatal("expected fires after regaining leadership")
	}

	// Not leader -> stops
	m.OnLeadershipChange(false)
	time.Sleep(300 * time.Millisecond)
	c4 := firedCount.Load()
	if c4 != c3 {
		t.Fatal("expected no fires after losing leadership again")
	}
}

func TestManagerDoesNotFireWithoutLeadership(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Don't call OnLeadershipChange(true) — should not fire
	time.Sleep(300 * time.Millisecond)

	if firedCount.Load() != 0 {
		t.Fatal("expected no fires without leadership")
	}
}

func TestManagerStop(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	if firedCount.Load() < 1 {
		t.Fatal("expected fires as leader")
	}

	// Stop the manager
	m.Stop()

	after := firedCount.Load()
	time.Sleep(300 * time.Millisecond)

	if firedCount.Load() != after {
		t.Fatal("expected no fires after Stop")
	}
}

func TestManagerJobHandoffOnLeadershipLoss(t *testing.T) {
	// Test the handoff scenario: leader A has jobs, leader drops,
	// node B takes over.
	// We simulate by tracking which node fired events.
	var nodeAFires atomic.Int32
	var nodeBFires atomic.Int32

	// Node A is leader initially
	nodeA := NewManager(func(e Event) {
		nodeAFires.Add(1)
	}, 50*time.Millisecond)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	nodeA.Start(ctxA)
	nodeA.SetJobs([]Job{
		{Name: "test", Command: "echo a", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Node B is standby
	nodeB := NewManager(func(e Event) {
		nodeBFires.Add(1)
	}, 50*time.Millisecond)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	nodeB.Start(ctxB)
	nodeB.SetJobs([]Job{
		{Name: "test", Command: "echo b", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Node A is leader
	nodeA.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	if nodeAFires.Load() < 1 {
		t.Fatal("expected node A to fire as leader")
	}
	if nodeBFires.Load() != 0 {
		t.Fatal("expected node B not to fire as standby")
	}

	// Node A drops — leadership transitions
	nodeA.OnLeadershipChange(false)

	// Node B becomes leader (simulating lowest-name election result)
	nodeB.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)

	if nodeBFires.Load() < 1 {
		t.Fatal("expected node B to fire after taking over leadership (job handoff)")
	}

	// Node A should not fire any more
	aAfter := nodeAFires.Load()
	time.Sleep(200 * time.Millisecond)
	if nodeAFires.Load() != aAfter {
		t.Fatal("expected node A not to fire after losing leadership")
	}
}

func TestManagerSetJobsHotReload(t *testing.T) {
	var events []Event
	done := make(chan struct{})
	m := NewManager(func(e Event) {
		events = append(events, e)
		close(done)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	// Set initial jobs and become leader
	m.SetJobs([]Job{
		{Name: "original", Command: "echo original", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})
	m.OnLeadershipChange(true)

	// Replace jobs while running
	m.SetJobs([]Job{
		{Name: "replaced", Command: "echo replaced", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	// Wait for the replaced job to fire
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for hot-reloaded job to fire")
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Name != "replaced" {
		t.Errorf("expected job name 'replaced', got %q", events[0].Name)
	}
}

func TestManagerStartAfterStopFire(t *testing.T) {
	var firedCount atomic.Int32
	m := NewManager(func(e Event) {
		firedCount.Add(1)
	}, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	m.SetJobs([]Job{
		{Name: "fast", Command: "echo fast", Timeout: time.Second, Schedule: &EverySchedule{Interval: 100 * time.Millisecond}},
	})

	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	if firedCount.Load() < 1 {
		t.Fatal("expected fires as leader")
	}

	// Stop and re-start scenario — only Stop is relevant
	m.Stop()
	after := firedCount.Load()

	// Try OnLeadershipChange after Stop — should be no-op
	m.OnLeadershipChange(true)
	time.Sleep(250 * time.Millisecond)
	if firedCount.Load() != after {
		t.Fatal("expected no fires after Stop even with OnLeadershipChange")
	}
}

func TestManagerJobsSnapshotImmutability(t *testing.T) {
	m := NewManager(nil, 0)

	jobs := []Job{
		{Name: "j1", Command: "c1", Timeout: time.Second, Schedule: &EverySchedule{Interval: time.Hour}},
	}
	m.SetJobs(jobs)

	// Modify the returned snapshot — should not affect manager
	got := m.Jobs()
	got[0].Name = "modified"

	original := m.Jobs()
	if original[0].Name != "j1" {
		t.Errorf("expected original job name 'j1', got %q", original[0].Name)
	}
}
