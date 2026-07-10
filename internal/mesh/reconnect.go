package mesh

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Default backoff constants.
const (
	defaultBackoffFactor = 2.0
	defaultBackoffJitter = 0.25  // ±25%
	cbCooldown           = 5 * time.Minute // how long before a tripped circuit resets
)

// BackoffConfig parameterizes the exponential backoff strategy.
type BackoffConfig struct {
	// Min is the initial delay before the first retry.
	Min time.Duration
	// Max is the upper bound on delay.
	Max time.Duration
	// Factor is the exponential multiplier (default 2.0).
	Factor float64
	// Jitter is the randomisation ratio, e.g. 0.25 means ±25% (default 0.25).
	Jitter float64
}

// DefaultBackoff returns a BackoffConfig with sensible defaults.
func DefaultBackoff() BackoffConfig {
	return BackoffConfig{
		Min:    1 * time.Second,
		Max:    60 * time.Second,
		Factor: defaultBackoffFactor,
		Jitter: defaultBackoffJitter,
	}
}

// Delay returns the backoff duration for the nth attempt (0-based).
// Uses exponential backoff with uniform jitter: base * factor^attempt, capped, then jittered.
func (b BackoffConfig) Delay(attempt int) time.Duration {
	capNanos := float64(b.Max.Nanoseconds())
	baseNanos := float64(b.Min.Nanoseconds())
	// delay = min(max, base * factor^attempt)
	exp := math.Pow(b.Factor, float64(attempt))
	d := baseNanos * exp
	if d > capNanos {
		d = capNanos
	}
	// Apply uniform jitter: ±jitter%
	jRange := d * b.Jitter
	d = d - jRange + (rand.Float64() * 2 * jRange)
	if d < 1 {
		d = 1 // floor at 1ns to avoid busy-loop
	}
	return time.Duration(d)
}

// trackedPeer holds per-peer reconnect state including backoff and circuit breaker.
type trackedPeer struct {
	addr        string
	stopCh      chan struct{} // closed to cancel reconnect loop
	attempt     int           // consecutive reconnect attempts (reset on success)
	circuitOpen bool          // circuit breaker tripped
	cbResetAt   time.Time     // when the circuit breaker will reset (cooldown)
}

// ReconnectManager handles automatic reconnection to dropped peers
// with exponential backoff, jitter, and an optional circuit breaker.
type ReconnectManager struct {
	mu               sync.Mutex
	tracked          map[string]*trackedPeer // name -> tracked info
	backoff          BackoffConfig
	cbLimit          int           // consecutive failures before circuit opens (0 = disabled)
	hub              *Hub
	selfName         string
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewReconnectManager creates a reconnect manager.
//   - hub: peer hub for reconnecting
//   - selfName: this node's name
//   - backoff: backoff configuration (Min/Max/Factor/Jitter)
//   - circuitBreakerLimit: consecutive failures before circuit opens (0 = disabled)
func NewReconnectManager(hub *Hub, selfName string, backoff BackoffConfig, circuitBreakerLimit int) *ReconnectManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReconnectManager{
		tracked:  make(map[string]*trackedPeer),
		backoff:  backoff,
		cbLimit:  circuitBreakerLimit,
		hub:      hub,
		selfName: selfName,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Track registers or updates a peer address for reconnection tracking.
func (rm *ReconnectManager) Track(name, addr string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if existing, ok := rm.tracked[name]; ok {
		existing.addr = addr
		// Cancel any active reconnect loop — peer is now connected
		if existing.stopCh != nil {
			close(existing.stopCh)
			existing.stopCh = nil
		}
		// Reset backoff and circuit breaker on successful connection
		existing.attempt = 0
		existing.circuitOpen = false
		existing.cbResetAt = time.Time{}
		return
	}
	rm.tracked[name] = &trackedPeer{addr: addr}
}

// Forget stops tracking a peer entirely and cancels any active reconnect.
func (rm *ReconnectManager) Forget(name string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if p, ok := rm.tracked[name]; ok {
		if p.stopCh != nil {
			close(p.stopCh)
		}
		delete(rm.tracked, name)
	}
}

// CancelReconnect cancels the active reconnect loop for a peer without
// forgetting the address. Used when the peer reconnects on its own.
func (rm *ReconnectManager) CancelReconnect(name string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if p, ok := rm.tracked[name]; ok && p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil
		// Reset attempt counter — successful reconnect
		p.attempt = 0
		p.circuitOpen = false
		p.cbResetAt = time.Time{}
	}
}

// OnDisconnect is called when a tracked peer disconnects unexpectedly.
// It starts a goroutine that retries the connection with exponential
// backoff + jitter and circuit breaker protection.
func (rm *ReconnectManager) OnDisconnect(name string) {
	rm.mu.Lock()
	p, ok := rm.tracked[name]
	if !ok {
		rm.mu.Unlock()
		return
	}
	// Don't stack multiple reconnect loops
	if p.stopCh != nil {
		rm.mu.Unlock()
		return
	}

	// Check circuit breaker
	if p.circuitOpen {
		if time.Now().Before(p.cbResetAt) {
			rm.mu.Unlock()
			slog.Warn("peer reconnect circuit open, skipping",
				"peer", name, "reset_at", p.cbResetAt.Format(time.RFC3339))
			return
		}
		// Cooldown expired — reset to half-open
		slog.Info("peer reconnect circuit breaker reset (cooldown expired)",
			"peer", name)
		p.circuitOpen = false
		p.attempt = 0
		p.cbResetAt = time.Time{}
	}

	stopCh := make(chan struct{})
	p.stopCh = stopCh
	addr := p.addr
	attempt := p.attempt
	rm.mu.Unlock()

	slog.Info("starting peer reconnect with backoff",
		"peer", name, "addr", addr,
		"attempt", attempt+1,
		"cb_limit", rm.cbLimit)

	go rm.reconnectLoop(name, addr, attempt, stopCh)
}

// Stop cancels all reconnect loops and stops the manager.
func (rm *ReconnectManager) Stop() {
	rm.cancel()
}

// Status returns a snapshot of tracked peers and their circuit breaker state.
func (rm *ReconnectManager) Status() map[string]string {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	out := make(map[string]string, len(rm.tracked))
	for name, p := range rm.tracked {
		if p.circuitOpen {
			out[name] = "circuit_open"
		} else if p.stopCh != nil {
			out[name] = "reconnecting"
		} else {
			out[name] = "connected"
		}
	}
	return out
}

func (rm *ReconnectManager) reconnectLoop(name, addr string, startAttempt int, stopCh chan struct{}) {
	attempt := startAttempt

	for {
		// Calculate delay with exponential backoff + jitter
		delay := rm.backoff.Delay(attempt)
		slog.Debug("reconnect scheduled",
			"peer", name, "addr", addr,
			"attempt", attempt+1,
			"delay", delay)

		select {
		case <-rm.ctx.Done():
			slog.Debug("reconnect loop stopped (shutdown)", "peer", name)
			return
		case <-stopCh:
			slog.Debug("reconnect cancelled", "peer", name)
			return
		case <-time.After(delay):
		}

		slog.Debug("attempting reconnect", "peer", name, "addr", addr, "attempt", attempt+1)
		peer, err := Connect(rm.ctx, addr, rm.selfName, rm.hub)
		if err != nil {
			if rm.ctx.Err() != nil {
				return
			}

			attempt++

			// Check circuit breaker
			if rm.cbLimit > 0 && attempt-startAttempt >= rm.cbLimit {
				rm.mu.Lock()
				// Only trip if this peer is still tracked and hasn't reconnected elsewhere
				if p, ok := rm.tracked[name]; ok && p.stopCh == stopCh {
					p.circuitOpen = true
					p.cbResetAt = time.Now().Add(cbCooldown)
					p.stopCh = nil // allow a fresh OnDisconnect to retry later
					slog.Warn("peer reconnect circuit breaker tripped",
						"peer", name, "addr", addr,
						"failures", attempt-startAttempt,
						"cooldown", cbCooldown)
				}
				rm.mu.Unlock()
				return
			}

			slog.Warn("reconnect failed",
				"peer", name, "addr", addr,
				"attempt", attempt,
				"err", err)
			continue
		}

		// Success! Peer reconnected.
		slog.Info("reconnect successful",
			"peer", name, "addr", addr,
			"attempts", attempt-startAttempt+1)

		// Update tracked state: reset backoff + circuit breaker
		rm.mu.Lock()
		if p, ok := rm.tracked[name]; ok && p.stopCh == stopCh {
			p.stopCh = nil
			p.attempt = 0
			p.circuitOpen = false
			p.cbResetAt = time.Time{}
		}
		rm.mu.Unlock()

		// Cancel any other reconnect attempt (shouldn't be any, but be safe)
		rm.CancelReconnect(name)
		_ = peer
		return
	}
}
