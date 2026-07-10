package cron

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Job represents a scheduled job.
type Job struct {
	Name         string
	Command      string
	Timeout      time.Duration
	Schedule     Schedule
	MaxRetries   int           // max retry attempts on failure (0 = no retry)
	RetryDelay   time.Duration // delay between retry attempts (default: 30s)
	CatchUpLimit int           // max missed firings to catch up on leadership change (0 = no catch-up)
}

// jobState tracks a job's runtime state inside the scheduler.
type jobState struct {
	Job
	next time.Time
}

// Event is fired when a job is due for execution.
type Event struct {
	// Name is the job name.
	Name string
	// Command is the shell command to execute.
	Command string
	// Timeout is the maximum execution duration.
	Timeout time.Duration
	// FiredAt is when the job was triggered.
	FiredAt time.Time
	// MaxRetries is the maximum retry attempts for this job.
	MaxRetries int
	// RetryAttempt is which retry this is (0 = first attempt).
	RetryAttempt int
	// RetryDelay is the delay between retry attempts.
	RetryDelay time.Duration
	// OnResult, if non-nil, is called by the handler when execution completes.
	// err is non-nil on failure. output is the command's stdout+stderr.
	// If OnResult is nil, the scheduler/manager cannot track completion or retry.
	OnResult func(err error, output string, duration time.Duration)
}

// EventHandler is called when a job fires.
type EventHandler func(Event)

// Scheduler manages cron jobs and fires events when jobs are due.
// It runs a background tick loop that checks every second.
type Scheduler struct {
	mu       sync.Mutex
	jobs     []*jobState
	handler  EventHandler
	tick     time.Duration
	done     chan struct{}
	started  bool
}

// NewScheduler creates a new cron scheduler.
// tick is the resolution of the scheduler loop (defaults to 1s if zero).
func NewScheduler(handler EventHandler, tick time.Duration) *Scheduler {
	if tick <= 0 {
		tick = time.Second
	}
	if handler == nil {
		handler = func(Event) {} // no-op
	}
	return &Scheduler{
		handler: handler,
		tick:    tick,
		done:    make(chan struct{}),
	}
}

// AddJob adds a job to the scheduler.
// If the scheduler is already running, the job's next fire time is calculated
// from the current time.
func (s *Scheduler) AddJob(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	js := &jobState{Job: job}
	js.next = job.Schedule.Next(time.Now())
	s.jobs = append(s.jobs, js)

	slog.Debug("cron job added",
		"name", job.Name,
		"command", job.Command,
		"next", js.next.Format(time.RFC3339),
	)
}

// ReplaceAll replaces all jobs atomically. Used by the Manager to hot-reload
// jobs while the scheduler is running.
func (s *Scheduler) ReplaceAll(jobs []Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = make([]*jobState, 0, len(jobs))
	now := time.Now()
	for _, j := range jobs {
		js := &jobState{Job: j}
		js.next = j.Schedule.Next(now)
		s.jobs = append(s.jobs, js)
	}
	slog.Debug("cron jobs replaced", "count", len(jobs))
}

// RemoveJob removes a job by name. Returns true if found and removed.
func (s *Scheduler) RemoveJob(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, js := range s.jobs {
		if js.Name == name {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			slog.Debug("cron job removed", "name", name)
			return true
		}
	}
	return false
}

// Jobs returns a snapshot of all registered jobs.
func (s *Scheduler) Jobs() []Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Job, len(s.jobs))
	for i, js := range s.jobs {
		out[i] = js.Job
	}
	return out
}

// Start begins the scheduler tick loop in a background goroutine.
// The loop runs until the context is cancelled or Stop is called.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	go s.loop(ctx)
}

// Stop stops the scheduler loop. It blocks until the loop exits.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Signal done and wait for loop to exit via context or manual stop
	close(s.done)
}

func (s *Scheduler) loop(ctx context.Context) {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	slog.Info("cron scheduler started", "tick", s.tick)

	for {
		select {
		case <-ctx.Done():
			slog.Info("cron scheduler stopped (context cancelled)")
			return
		case <-s.done:
			slog.Info("cron scheduler stopped")
			return
		case now := <-ticker.C:
			s.fireDue(now)
		}
	}
}

// fireDue checks all jobs and fires any that are due.
func (s *Scheduler) fireDue(now time.Time) {
	s.mu.Lock()
	// Collect due jobs and update their next times.
	type due struct {
		job Job
		at  time.Time
	}
	var dueJobs []due

	for _, js := range s.jobs {
		if !js.next.IsZero() && (now.Equal(js.next) || now.After(js.next)) {
			dueJobs = append(dueJobs, due{job: js.Job, at: js.next})
			// Calculate next occurrence
			js.next = js.Schedule.Next(js.next)
		}
	}
	s.mu.Unlock()

	// Fire events outside the lock.
	for _, d := range dueJobs {
		slog.Info("cron job firing",
			"name", d.job.Name,
			"command", d.job.Command,
			"scheduled", d.at.Format(time.RFC3339),
		)
		s.handler(Event{
			Name:         d.job.Name,
			Command:      d.job.Command,
			Timeout:      d.job.Timeout,
			FiredAt:      d.at,
			MaxRetries:   d.job.MaxRetries,
			RetryDelay:   d.job.RetryDelay,
			OnResult:     nil,
		})
	}
}
