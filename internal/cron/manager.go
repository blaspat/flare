package cron

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DefaultCatchUpLookback is how far back to check for missed jobs on leadership change.
// 0 means disabled by default — set via SetCatchUpLookback to enable.
const DefaultCatchUpLookback = 0

// DefaultRetryDelay is the default delay between retry attempts.
const DefaultRetryDelay = 30 * time.Second

// Manager manages the lifecycle of the cron scheduler based on leadership.
//
// When this node becomes the leader, the scheduler starts running configured jobs.
// When leadership is lost (leader node drops, lower-name node joins), the scheduler
// stops — the new leader takes over and execution continues there.
//
// HA features:
//   - Job history: ring buffer of execution records (audit log)
//   - Catch-up on leadership change: fires missed jobs within a lookback window
//   - Retry policy: retries failed jobs up to MaxRetries times with RetryDelay
type Manager struct {
	mu              sync.Mutex
	configJobs      []Job
	handler         EventHandler
	scheduler       *Scheduler
	isLeader        bool
	ctx             context.Context
	started         bool
	tick            time.Duration
	catchUpLookback time.Duration
	nodeName        string
	history         *History
}

// NewManager creates a cron manager. handler is called on each job fire when
// this node is the leader. Pass nil for a no-op handler.
// tick is the scheduler resolution (defaults to 1s if zero).
func NewManager(handler EventHandler, tick time.Duration) *Manager {
	if handler == nil {
		handler = func(Event) {}
	}
	return &Manager{
		handler:         handler,
		tick:            tick,
		catchUpLookback: DefaultCatchUpLookback,
		history:         NewHistory(100),
	}
}

// SetNodeName sets the node name for history tracking.
func (m *Manager) SetNodeName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeName = name
}

// SetCatchUpLookback sets how far back to look for missed jobs on leadership change.
// Set to 0 to disable catch-up entirely.
// Default is 0 (disabled).
func (m *Manager) SetCatchUpLookback(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catchUpLookback = d
}

// SetHistoryMax sets the maximum number of history entries. Default is 100.
func (m *Manager) SetHistoryMax(max int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.history = NewHistory(max)
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
// Also runs catch-up for missed jobs on gaining leadership.
func (m *Manager) OnLeadershipChange(isLeader bool) {
	m.mu.Lock()
	if !m.started || isLeader == m.isLeader {
		m.mu.Unlock()
		return
	}
	m.isLeader = isLeader
	jobs := make([]Job, len(m.configJobs))
	copy(jobs, m.configJobs)
	catchUpLookback := m.catchUpLookback
	nodeName := m.nodeName
	m.mu.Unlock()

	if isLeader {
		now := time.Now()

		// Run catch-up for missed jobs before starting the regular scheduler.
		if catchUpLookback > 0 {
			lookbackStart := now.Add(-catchUpLookback)
			m.catchUpMissed(jobs, lookbackStart, now, nodeName)
		}

		m.startScheduler()
	} else {
		m.stopScheduler()
	}
}

// catchUpMissed fires any job executions that were missed between since and now.
// Each job fires at most CatchUpLimit times (default 1 if not set).
func (m *Manager) catchUpMissed(jobs []Job, since, now time.Time, nodeName string) {
	for _, job := range jobs {
		limit := job.CatchUpLimit
		if limit <= 0 {
			limit = 1 // default: catch up the most recent missed firing
		}

		missed := computeMissedFirings(job, since, now, limit)
		for _, firedAt := range missed {
			slog.Info("cron catch-up firing missed job",
				"name", job.Name,
				"original_scheduled", firedAt.Format(time.RFC3339),
				"now", now.Format(time.RFC3339),
			)
			m.recordAndFire(Event{
				Name:         job.Name,
				Command:      job.Command,
				Timeout:      job.Timeout,
				FiredAt:      firedAt,
				MaxRetries:   job.MaxRetries,
				RetryDelay:   job.RetryDelay,
				OnResult:     m.makeOnResult(job, nodeName),
			}, nodeName)
		}
	}
}

// computeMissedFirings finds firing times for a job between since and now, up to limit.
func computeMissedFirings(job Job, since, now time.Time, limit int) []time.Time {
	if limit <= 0 {
		return nil
	}
	if now.Before(since) {
		return nil
	}

	var result []time.Time

	switch s := job.Schedule.(type) {
	case *EverySchedule:
		if s.Interval <= 0 {
			return nil
		}
		// For EverySchedule, compute the number of intervals between since and now.
		elapsed := now.Sub(since)
		count := int(elapsed / s.Interval)
		if count > limit {
			count = limit
		}
		for i := count; i > 0; i-- {
			t := now.Add(-time.Duration(i) * s.Interval)
			if !t.Before(since) {
				result = append(result, t)
			}
		}

	case *CronSchedule:
		// For CronSchedule, walk forward using Next() and collect.
		t := s.Next(since)
		for !t.IsZero() && !t.After(now) && len(result) < limit {
			result = append(result, t)
			t = s.Next(t)
		}
	}

	return result
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

// History returns a snapshot of recent job execution records (newest first).
func (m *Manager) History() []HistoryEntry {
	m.mu.Lock()
	h := m.history
	m.mu.Unlock()
	if h == nil {
		return nil
	}
	return h.Snapshot()
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

	s := NewScheduler(m.wrappedHandler(), m.tick)
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

// wrappedHandler returns an EventHandler that wraps the user's handler with
// history tracking and retry support.
func (m *Manager) wrappedHandler() EventHandler {
	return func(e Event) {
		m.mu.Lock()
		nodeName := m.nodeName
		m.mu.Unlock()

		m.recordAndFire(e, nodeName)
	}
}

// recordAndFire calls the real handler and records the result in history.
func (m *Manager) recordAndFire(e Event, nodeName string) {
	// If OnResult is nil, attach one for tracking
	onResult := e.OnResult
	if onResult == nil {
		onResult = func(err error, output string, duration time.Duration) {
			// Default: just log
			if err != nil {
				slog.Error("cron job failed", "name", e.Name, "err", err)
			} else {
				slog.Debug("cron job completed", "name", e.Name, "duration", duration)
			}
		}
	}

	// Wrap OnResult with history tracking
	wrappedOnResult := func(err error, output string, duration time.Duration) {
		// Record in history
		entry := HistoryEntry{
			Name:         e.Name,
			FiredAt:      e.FiredAt,
			CompletedAt:  time.Now(),
			Duration:     duration,
			Success:      err == nil,
			Output:       truncateOutput(output),
			RetryAttempt: e.RetryAttempt,
			LeaderNode:   nodeName,
		}
		if err != nil {
			entry.ErrMsg = err.Error()
		}

		m.mu.Lock()
		if m.history != nil {
			m.history.Append(entry)
		}
		maxRetries := e.MaxRetries
		m.mu.Unlock()

		// Call the original OnResult
		onResult(err, output, duration)

		// Retry logic: if failed and retries remain, schedule a retry
		if err != nil && e.RetryAttempt < maxRetries {
			retryDelay := e.RetryDelay
			if retryDelay <= 0 {
				retryDelay = DefaultRetryDelay
			}
			nextAttempt := e.RetryAttempt + 1

			slog.Info("cron job retry scheduled",
				"name", e.Name,
				"attempt", nextAttempt,
				"max_retries", maxRetries,
				"delay", retryDelay,
			)

			// Schedule retry in background
			go func() {
				select {
				case <-time.After(retryDelay):
					// Re-fire the job as a retry
					retryEvent := Event{
						Name:         e.Name,
						Command:      e.Command,
						Timeout:      e.Timeout,
						FiredAt:      time.Now(),
						MaxRetries:   maxRetries,
						RetryAttempt: nextAttempt,
						RetryDelay:   retryDelay,
						OnResult:     e.OnResult, // pass the original OnResult through
					}
					// Call the user's handler directly (not through scheduler)
					// But we need to wrap it again for history tracking...
					// Use a fresh recordAndFire call
					m.mu.Lock()
					nodeName := m.nodeName
					handler := m.handler
					m.mu.Unlock()

					// Create a new event with our wrapped OnResult for history tracking
					retryEvent.OnResult = m.makeOnResult(Job{
						Name:       e.Name,
						Command:    e.Command,
						Timeout:    e.Timeout,
						MaxRetries: maxRetries,
						RetryDelay: retryDelay,
					}, nodeName)

					handler(retryEvent)

				case <-m.ctx.Done():
					return
				}
			}()
		}
	}

	// Fire with the tracking wrapper
	fireEvent := e
	fireEvent.OnResult = wrappedOnResult
	fireEvent.RetryAttempt = e.RetryAttempt

	m.mu.Lock()
	handler := m.handler
	m.mu.Unlock()

	handler(fireEvent)
}

// makeOnResult creates an OnResult callback that records history for a job.
func (m *Manager) makeOnResult(job Job, nodeName string) func(err error, output string, duration time.Duration) {
	return func(err error, output string, duration time.Duration) {
		entry := HistoryEntry{
			Name:         job.Name,
			FiredAt:      time.Now(),
			CompletedAt:  time.Now(),
			Duration:     duration,
			Success:      err == nil,
			Output:       truncateOutput(output),
			RetryAttempt: 0,
			LeaderNode:   nodeName,
		}
		if err != nil {
			entry.ErrMsg = err.Error()
		}

		m.mu.Lock()
		if m.history != nil {
			m.history.Append(entry)
		}
		m.mu.Unlock()

		if err != nil {
			slog.Error("cron job failed", "name", job.Name, "err", err)
		}
	}
}

// truncateOutput truncates output to 1024 bytes for history storage.
func truncateOutput(output string) string {
	if len(output) > 1024 {
		return output[:1024] + "..."
	}
	return output
}
