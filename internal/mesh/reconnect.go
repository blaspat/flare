package mesh

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ReconnectManager handles automatic reconnection to dropped peers.
// It tracks peer addresses and retries connection at a fixed interval.
type ReconnectManager struct {
	mu       sync.Mutex
	tracked  map[string]*trackedPeer // name -> tracked info
	interval time.Duration
	hub      *Hub
	selfName string
	ctx      context.Context
	cancel   context.CancelFunc
}

type trackedPeer struct {
	addr   string
	stopCh chan struct{} // closed to cancel an active reconnect loop
}

// NewReconnectManager creates a reconnect manager.
func NewReconnectManager(hub *Hub, selfName string, interval time.Duration) *ReconnectManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReconnectManager{
		tracked:  make(map[string]*trackedPeer),
		interval: interval,
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
		return
	}
	rm.tracked[name] = &trackedPeer{addr: addr}
}

// Forget stops tracking a peer entirely and cancels any active reconnect.
// Used when a peer is intentionally forgotten.
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
	}
}

// OnDisconnect is called when a tracked peer disconnects unexpectedly.
// It starts a goroutine that retries the connection at the configured interval.
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
	stopCh := make(chan struct{})
	p.stopCh = stopCh
	addr := p.addr
	rm.mu.Unlock()

	slog.Info("starting peer reconnect", "peer", name, "addr", addr, "interval", rm.interval)
	go rm.reconnectLoop(name, addr, stopCh)
}

// Stop cancels all reconnect loops and stops the manager.
func (rm *ReconnectManager) Stop() {
	rm.cancel()
}

func (rm *ReconnectManager) reconnectLoop(name, addr string, stopCh chan struct{}) {
	ticker := time.NewTicker(rm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			slog.Debug("reconnect loop stopped (shutdown)", "peer", name)
			return
		case <-stopCh:
			slog.Debug("reconnect cancelled", "peer", name)
			return
		case <-ticker.C:
			slog.Debug("attempting reconnect", "peer", name, "addr", addr)
			peer, err := Connect(rm.ctx, addr, rm.selfName, rm.hub)
			if err != nil {
				if rm.ctx.Err() != nil {
					return
				}
				slog.Warn("reconnect failed", "peer", name, "addr", addr, "err", err)
				continue
			}
			slog.Info("reconnect successful", "peer", name, "addr", addr)

			// Cancel any other reconnect attempt (shouldn't be any, but be safe)
			rm.CancelReconnect(name)
			_ = peer
			return
		}
	}
}
