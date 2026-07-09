package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseScheduleEvery(t *testing.T) {
	tests := []struct {
		spec     string
		interval time.Duration
	}{
		{"@every 30s", 30 * time.Second},
		{"@every 5m", 5 * time.Minute},
		{"@every 1h", time.Hour},
		{"@every 500ms", 500 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			s, err := ParseSchedule(tc.spec)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			es, ok := s.(*EverySchedule)
			if !ok {
				t.Fatalf("expected *EverySchedule, got %T", s)
			}
			if es.Interval != tc.interval {
				t.Fatalf("expected interval %s, got %s", tc.interval, es.Interval)
			}
		})
	}
}

func TestParseScheduleEveryErrors(t *testing.T) {
	tests := []struct {
		spec string
		desc string
	}{
		{"@every 0s", "zero duration"},
		{"@every -5s", "negative duration"},
		{"@every ", "missing duration"},
		{"@every blah", "invalid duration"},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ParseSchedule(tc.spec)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestParseCronStar(t *testing.T) {
	s, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// All fields should match all values
	for i := 0; i < 60; i++ {
		if !s.minute[i] {
			t.Errorf("minute %d should be set", i)
		}
	}
}

func TestParseCronSingleValue(t *testing.T) {
	s, err := ParseCron("30 14 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s.minute[30] {
		t.Error("minute 30 should be set")
	}
	if !s.hour[14] {
		t.Error("hour 14 should be set")
	}
	// Ensure others are not set
	checkOnlyBitSet(t, s.minute[:60], 30, "minute")
	checkOnlyBitSet(t, s.hour[:24], 14, "hour")
}

func TestParseCronStep(t *testing.T) {
	s, err := ParseCron("*/15 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 60; i++ {
		expected := i%15 == 0
		if s.minute[i] != expected {
			t.Errorf("minute %d: expected %v, got %v", i, expected, s.minute[i])
		}
	}
}

func TestParseCronRange(t *testing.T) {
	s, err := ParseCron("9-17 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 60; i++ {
		expected := i >= 9 && i <= 17
		if s.minute[i] != expected {
			t.Errorf("minute %d: expected %v, got %v", i, expected, s.minute[i])
		}
	}
}

func TestParseCronList(t *testing.T) {
	s, err := ParseCron("0,15,30,45 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := map[int]bool{0: true, 15: true, 30: true, 45: true}
	for i := 0; i < 60; i++ {
		if s.minute[i] != expected[i] {
			t.Errorf("minute %d: expected %v, got %v", i, expected[i], s.minute[i])
		}
	}
}

func TestParseCronErrors(t *testing.T) {
	tests := []struct {
		expr string
		desc string
	}{
		{"", "empty"},
		{"* * *", "too few fields"},
		{"* * * * * *", "too many fields"},
		{"a * * * *", "invalid minute"},
		{"* 99 * * *", "hour out of bounds"},
		{"* * 32 * *", "dom out of bounds"},
		{"* * * 13 *", "month out of bounds"},
		{"* * * * 7", "dow out of bounds"},
		{"*/0 * * * *", "step zero"},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ParseCron(tc.expr)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestCronNextSimple(t *testing.T) {
	s, err := ParseCron("30 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// At 10:00, next should be 10:30
	base := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	next := s.Next(base)
	expected := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}

	// At 10:31, next should be 11:30
	base = time.Date(2025, 1, 15, 10, 31, 0, 0, time.UTC)
	next = s.Next(base)
	expected = time.Date(2025, 1, 15, 11, 30, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}
}

func TestCronNextDaily(t *testing.T) {
	s, err := ParseCron("0 9 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// At 10:00, next should be tomorrow 9:00
	base := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	next := s.Next(base)
	expected := time.Date(2025, 1, 16, 9, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}
}

func TestCronNextWeekly(t *testing.T) {
	// Every Monday at 0:00 (Monday = 1 in Go's Weekday)
	s, err := ParseCron("0 0 * * 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A Wednesday (2025-01-15 is a Wednesday)
	base := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	next := s.Next(base)
	// Next Monday is 2025-01-20
	expected := time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}
}

func TestCronNextMonthly(t *testing.T) {
	s, err := ParseCron("0 0 1 * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Jan 15 -> next Feb 1
	base := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	next := s.Next(base)
	expected := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}
}

func TestCronStepRange(t *testing.T) {
	s, err := ParseCron("0-30/15 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i <= 30; i++ {
		expected := i%15 == 0
		if s.minute[i] != expected {
			t.Errorf("minute %d: expected %v, got %v", i, expected, s.minute[i])
		}
	}
	if s.minute[31] {
		t.Error("minute 31 should not be set")
	}
}

func TestEveryNext(t *testing.T) {
	s := &EverySchedule{Interval: 30 * time.Second}
	base := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	next := s.Next(base)
	expected := time.Date(2025, 1, 15, 10, 0, 30, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next)
	}
}

func TestSchedulerAddRemoveJobs(t *testing.T) {
	s := NewScheduler(nil, time.Second)
	defer s.Stop()

	s.AddJob(Job{
		Name: "test-job", Command: "echo hello", Timeout: time.Minute,
		Schedule: &EverySchedule{Interval: 30 * time.Second},
	})

	jobs := s.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Name != "test-job" {
		t.Fatalf("expected job name 'test-job', got %q", jobs[0].Name)
	}

	if !s.RemoveJob("test-job") {
		t.Fatal("expected RemoveJob to return true")
	}
	if len(s.Jobs()) != 0 {
		t.Fatal("expected 0 jobs after removal")
	}
}

func TestSchedulerRemoveNonExistent(t *testing.T) {
	s := NewScheduler(nil, time.Second)
	if s.RemoveJob("nonexistent") {
		t.Fatal("expected RemoveJob to return false for non-existent job")
	}
}

func TestSchedulerFiresJob(t *testing.T) {
	var firedCount atomic.Int32
	handler := func(e Event) {
		firedCount.Add(1)
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.AddJob(Job{
		Name: "fast-job", Command: "echo fast", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 100 * time.Millisecond},
	})

	s.Start(ctx)

	// Wait for at least one fire
	time.Sleep(250 * time.Millisecond)
	cancel()

	count := firedCount.Load()
	if count < 1 {
		t.Fatalf("expected at least 1 fire, got %d", count)
	}
}

func TestSchedulerFiresCorrectJob(t *testing.T) {
	var events []Event
	done := make(chan struct{})
	handler := func(e Event) {
		events = append(events, e)
		close(done)
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.AddJob(Job{
		Name: "exact-job", Command: "echo hi", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 100 * time.Millisecond},
	})

	s.Start(ctx)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for job fire")
	}

	cancel()

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Name != "exact-job" {
		t.Fatalf("expected job name 'exact-job', got %q", events[0].Name)
	}
	if events[0].Command != "echo hi" {
		t.Fatalf("expected command 'echo hi', got %q", events[0].Command)
	}
}

func TestSchedulerDoesNotFireBeforeDue(t *testing.T) {
	var firedCount atomic.Int32
	handler := func(e Event) {
		firedCount.Add(1)
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schedule a job that fires every hour — should NOT fire
	s.AddJob(Job{
		Name: "slow-job", Command: "echo slow", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: time.Hour},
	})

	s.Start(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()

	if firedCount.Load() != 0 {
		t.Fatalf("expected 0 fires for hourly job, got %d", firedCount.Load())
	}
}

func TestSchedulerMultipleJobs(t *testing.T) {
	var firedCount atomic.Int32
	handler := func(e Event) {
		firedCount.Add(1)
	}

	s := NewScheduler(handler, 25*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add two fast jobs
	s.AddJob(Job{
		Name: "job-a", Command: "echo a", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 50 * time.Millisecond},
	})
	s.AddJob(Job{
		Name: "job-b", Command: "echo b", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 50 * time.Millisecond},
	})

	s.Start(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	count := firedCount.Load()
	if count < 2 {
		t.Fatalf("expected at least 2 fires (2 jobs), got %d", count)
	}
}

func TestSchedulerContextCancellation(t *testing.T) {
	var firedCount atomic.Int32
	handler := func(e Event) {
		firedCount.Add(1)
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	s.AddJob(Job{
		Name: "cancel-test", Command: "echo test", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 50 * time.Millisecond},
	})

	s.Start(ctx)

	// Let it fire a few times
	time.Sleep(120 * time.Millisecond)

	// Cancel context
	cancel()

	// Give it a moment to stop
	time.Sleep(100 * time.Millisecond)

	afterCancel := firedCount.Load()

	// Wait a bit more to ensure it stopped
	time.Sleep(200 * time.Millisecond)

	if firedCount.Load() != afterCancel {
		t.Fatalf("scheduler fired after cancellation: before=%d, after=%d", afterCancel, firedCount.Load())
	}
}

func TestSchedulerStop(t *testing.T) {
	var firedCount atomic.Int32
	handler := func(e Event) {
		firedCount.Add(1)
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx := context.Background()

	s.AddJob(Job{
		Name: "stop-test", Command: "echo test", Timeout: time.Second,
		Schedule: &EverySchedule{Interval: 50 * time.Millisecond},
	})

	s.Start(ctx)

	time.Sleep(120 * time.Millisecond)
	s.Stop()

	afterStop := firedCount.Load()
	time.Sleep(200 * time.Millisecond)

	if firedCount.Load() != afterStop {
		t.Fatalf("scheduler fired after Stop: before=%d, after=%d", afterStop, firedCount.Load())
	}
}

func TestSchedulerEventFields(t *testing.T) {
	got := make(chan Event, 1)
	handler := func(e Event) {
		got <- e
	}

	s := NewScheduler(handler, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.AddJob(Job{
		Name: "field-test", Command: "test-command", Timeout: 5 * time.Second,
		Schedule: &EverySchedule{Interval: 100 * time.Millisecond},
	})

	s.Start(ctx)

	var ev Event
	select {
	case ev = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if ev.Name != "field-test" {
		t.Errorf("expected Name='field-test', got %q", ev.Name)
	}
	if ev.Command != "test-command" {
		t.Errorf("expected Command='test-command', got %q", ev.Command)
	}
	if ev.Timeout != 5*time.Second {
		t.Errorf("expected Timeout=5s, got %s", ev.Timeout)
	}
	if ev.FiredAt.IsZero() {
		t.Error("FiredAt should not be zero")
	}
}

// helpers

func checkOnlyBitSet(t *testing.T, bits []bool, index int, name string) {
	t.Helper()
	for i, b := range bits {
		if i == index && !b {
			t.Errorf("%s[%d] should be set", name, i)
		}
		if i != index && b {
			t.Errorf("%s[%d] should not be set", name, i)
		}
	}
}
