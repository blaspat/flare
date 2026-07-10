# Distributed Cron HA

Three features for making Flare's distributed cron scheduler more reliable:

## 1. Job History / Audit Log

A thread-safe ring buffer (`History` in `history.go`) that records every job execution:

```go
type HistoryEntry struct {
    Name         string        // job name
    FiredAt      time.Time     // when the job was triggered
    CompletedAt  time.Time     // when execution finished
    Duration     time.Duration // wall-clock duration
    Success      bool          // nil error = success
    ErrMsg       string        // error message on failure
    Output       string        // last 1KB of stdout+stderr
    RetryAttempt int           // 0 = first attempt
    LeaderNode   string        // which node executed the job
}
```

- **Configurable size** via `history_size` (default: 100)
- **Newest-first** ordering on `Snapshot()` and `Recent(N)`
- **Ring buffer** — oldest entries are overwritten when full
- **Thread-safe** via mutex
- **Accessed** via `Manager.History() []HistoryEntry`
- **Truncates output** to 1024 bytes for storage efficiency

### Integration in CLI handler

The cron event handler calls `e.OnResult(err, output, 0)` to report completion. The Manager wraps this to record history entries automatically. The handler in `cli.go` now always calls `e.OnResult` regardless of success or failure.

## 2. Missed-Job Catch-Up on Leader Election

When a new leader takes over, it checks if any jobs were missed during the transition and fires them.

### Configuration

```toml
[cron]
catch_up_lookback = "5m"  # how far back to check (default: 0 = disabled)
```

Per-job:
```toml
[[cron.jobs]]
catch_up_limit = 1  # max missed firings to catch up per job (default: 1)
```

### Algorithm

- `SetCatchUpLookback(d)` enables catch-up with the given lookback window
- On leadership gain (`OnLeadershipChange(true)`):
  1. Compute `lookbackStart = now - catchUpLookback`
  2. For each job with `CatchUpLimit > 0`, call `computeMissedFirings()`
  3. Fire each missed firing immediately via the wrapped handler (for history tracking)

### `computeMissedFirings`

- **EverySchedule**: `count = min(elapsed/interval, limit)`. Returns the last `count` firing times.
- **CronSchedule**: Walks forward using `Schedule.Next()` from `lookbackStart` to `now`, collecting up to `limit` times.
- Returns empty slice when `limit <= 0` or `since > now`.

## 3. Configurable Job Retry Policy

When a job fails, the Manager can retry it after a delay.

### Configuration

Per-job:
```toml
[[cron.jobs]]
retry_count = 2      # max retry attempts (0 = no retry, default)
retry_delay = "10s"  # delay between retries (default: 30s)
```

### Mechanism

- The Manager's wrapped handler (`wrappedHandler`) records every execution outcome
- When `OnResult(err, ...)` is called with a non-nil error and `e.RetryAttempt < e.MaxRetries`:
  1. Log the retry attempt
  2. Spawn a background goroutine that waits `RetryDelay` then re-fires the job
  3. The retry event increments `RetryAttempt` and carries the original `OnResult`
- **Thread-safe** — retry goroutines are independent of each other and of the scheduler loop
- **Context-aware** — retry goroutines exit when the Manager's context is cancelled

### Design Decisions

- **OnResult callback** — the `Event` struct carries an optional `OnResult func(err error, output string, duration time.Duration)`. The handler calls it when execution completes. If nil, no tracking occurs (backward-compatible).
- **Opt-in catch-up** — `CatchUpLookback` defaults to 0 (disabled). Set explicitly via `SetCatchUpLookback()` or config option.
- **History tracking without OnResult** — the Manager wraps the user's handler. Even if the original Event doesn't have OnResult, the wrapper creates one for history tracking.
- **Retry fires directly** — retries fire the handler directly, not through the scheduler. This avoids re-checking the schedule and ensures timely retry.

### Test Pattern

- History: append, snapshot, ring wrap, clear, recent N, empty
- ComputeMissedFirings: EverySchedule + CronSchedule with various limits
- Catch-up integration: enable catch-up, become leader, verify events fire
- ManagerHistory: become leader, let jobs fire, check history entries
- ManagerHistory failure: report error via OnResult, verify failure record
