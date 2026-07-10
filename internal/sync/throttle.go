// Package sync — bandwidth throttling for file transfers.
package sync

import (
	"context"
	"math"
	"sync"
	"time"
)

// Throttler implements a token-bucket rate limiter for outgoing file chunks.
// It limits the number of bytes that can be sent per second, with configurable
// burst capacity.
//
// Zero value is invalid — use NewThrottler to create one.
type Throttler struct {
	rate   float64 // bytes per second
	burst  float64 // max accumulated tokens (capacity)
	tokens float64 // current token balance
	lastNS int64   // unix nano of last refill

	mu   sync.Mutex
	cond *sync.Cond
}

// NewThrottler creates a token bucket that limits throughput to rate bytes/sec
// with the given burst capacity. If burst is 0, it defaults to rate (one
// second's worth). If rate is 0 or negative an unlimited throttler is returned.
func NewThrottler(rateBytesPerSec int64, burst int64) *Throttler {
	if rateBytesPerSec <= 0 {
		return &Throttler{rate: math.Inf(1), tokens: math.Inf(1)}
	}
	if burst <= 0 {
		burst = rateBytesPerSec
	}
	t := &Throttler{
		rate:   float64(rateBytesPerSec),
		burst:  float64(burst),
		tokens: float64(burst),
		lastNS: time.Now().UnixNano(),
	}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// WaitN blocks until n tokens are available (or ctx is cancelled).
// It returns ctx.Err() if the context expires before enough tokens accumulate.
// If the throttler is unlimited (rate <= 0) this returns immediately.
func (t *Throttler) WaitN(ctx context.Context, n int) error {
	if math.IsInf(t.rate, 1) || n <= 0 {
		return nil
	}

	need := float64(n)
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		t.refillLocked()
		if t.tokens >= need {
			t.tokens -= need
			return nil
		}

		// Not enough tokens — calculate wait time.
		deficit := need - t.tokens
		waitNS := int64(math.Ceil(deficit / t.rate * 1e9))

		// Early exit channel setup (unlock while waiting).
		t.mu.Unlock()

		var timer *time.Timer
		select {
		case <-ctx.Done():
			t.mu.Lock()
			return ctx.Err()
		case <-func() <-chan time.Time {
			timer = time.NewTimer(time.Duration(waitNS))
			return timer.C
		}():
		}

		t.mu.Lock()
		if timer != nil {
			timer.Stop()
		}
		// Loop back — recalculate after refill.
	}
}

// refillLocked adds tokens based on elapsed time since last refill.
// Must be called with t.mu held.
func (t *Throttler) refillLocked() {
	now := time.Now().UnixNano()
	elapsedNS := now - t.lastNS
	if elapsedNS <= 0 {
		return
	}
	t.lastNS = now
	t.tokens += float64(elapsedNS) / 1e9 * t.rate
	if t.tokens > t.burst {
		t.tokens = t.burst
	}
}

// Rate returns the configured rate in bytes/sec.
func (t *Throttler) Rate() int64 {
	if math.IsInf(t.rate, 1) {
		return 0
	}
	return int64(t.rate)
}

// Available returns the current token count (approximate, without locking).
func (t *Throttler) Available() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refillLocked()
	return t.tokens
}

// Unlimited returns a throttler that never blocks (rate = 0).
func Unlimited() *Throttler {
	return NewThrottler(0, 0)
}
