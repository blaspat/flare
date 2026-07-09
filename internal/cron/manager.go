package cron

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Manager manages the lifecycle of the cron scheduler based on leadership.
//
// When this node becomes the leader, the scheduler starts running configured jobs.
// When leadership is lost (leader node drops, lower-name node joins), the scheduler
// stops — the new leader takes over and execution continues there.
// This provides job handoff without any coordination messages: every node computes
// the same leader given the same member set (lowest-name algorithm), so the new
// leader independently starts its scheduler.
type Manager struct {
	mu         sync.Mutex
	configJobs []Job
	handler    EventHandler
	scheduler  *Scheduler
	isLeader   bool
	ctx        context.Context
	started    bool
	tick       time.Duration
}

// NewManager creates a cron manager. handler is called on each job fire when
// this node is the leader. Pass nil for a no-op handler.
// tick is the scheduler resolution (defaults to 1s if zero).
func NewManager(handler EventHandler, tick time.Duration) *Manager {
	if handler == nil {
		handler = func(Event) {}
	}
	return &Manager{
		handler: handler,
		tick:    tick,
	}
}

// Start initialises the manager. Must be called before OnLeadershipChange.
// ctx is passed to the scheduler's background loop when it starts.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	m.started = true
}

// SetJobs sets the configured jobs. These are the jobs the leader will execute.
// If the scheduler is already running (this node is leader), the jobs are
// reloaded immediately.
func (m *Manager) SetJobs(jobs []Job) {
	m.mu.Lock()
	m.configJobs = jobs
	sched := m.scheduler
	m.mu.Unlock()

	slog.Debug("cron manager jobs updated", "count", len(jobs))

	// If scheduler is running, replace its jobs
	if sched != nil {
		sched.ReplaceAll(jobs)
	}
}

// OnLeadershipChange handles a leadership transition.
// When this node becomes leader, the scheduler starts running jobs.
// When leadership is lost, the scheduler stops — the new leader takes over.
// Safe to call multiple times with the same value.
func (m *Manager) OnLeadershipChange(isLeader bool) {
	m.mu.Lock()
	if !m.started || isLeader == m.isLeader {
		m.mu.Unlock()
		return
	}
	m.isLeader = isLeader
	m.mu.Unlock()

	if isLeader {
		m.startScheduler()
	} else {
		m.stopScheduler()
	}
}

// IsLeader returns whether this node currently holds leadership.
func (m *Manager) IsLeader() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isLeader
}

// Jobs returns a snapshot of the configured jobs.
func (m *Manager) Jobs() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, len(m.configJobs))
	copy(out, m.configJobs)
	return out
}

// Stop stops the scheduler if running and prevents further leadership transitions.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = false
	if m.scheduler != nil {
		m.scheduler.Stop()
		m.scheduler = nil
	}
}

func (m *Manager) startScheduler() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Don't start if stopped since the check
	if !m.started {
		return
	}

	s := NewScheduler(m.handler, m.tick)
	for _, j := range m.configJobs {
		s.AddJob(j)
	}
	s.Start(m.ctx)
	m.scheduler = s
	slog.Info("cron scheduler started — executing jobs as leader",
		"job_count", len(m.configJobs))
}

func (m *Manager) stopScheduler() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.scheduler != nil {
		m.scheduler.Stop()
		m.scheduler = nil
		slog.Info("cron scheduler stopped — standby mode (not leader)")
	}
}
