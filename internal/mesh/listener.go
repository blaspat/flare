package mesh

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// ConnHandler is called when a new WebSocket connection is upgraded.
type ConnHandler func(conn *websocket.Conn)

// Listener accepts incoming WebSocket connections.
type Listener struct {
	server   *http.Server
	upgrader websocket.Upgrader
	onConn   ConnHandler
}

// NewListener creates a WebSocket listener that calls onConn for each upgrade.
func NewListener(addr string, onConn ConnHandler) *Listener {
	return &Listener{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
		onConn: onConn,
		server: &http.Server{
			Addr: addr,
		},
	}
}

// Start begins listening for WebSocket connections.
// Blocks until the context is cancelled.
func (l *Listener) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/mesh", l.handleConn)
	l.server.Handler = mux

	slog.Info("mesh listener starting", "addr", l.server.Addr)

	errCh := make(chan error, 1)
	go func() {
		if err := l.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down mesh listener")
		return l.server.Close()
	case err := <-errCh:
		return err
	}
}

func (l *Listener) handleConn(w http.ResponseWriter, r *http.Request) {
	conn, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("websocket upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}

	l.onConn(conn)
}

// Hub manages all active peer connections.
type Hub struct {
	mu       sync.RWMutex
	peers    map[string]*PeerState // name -> peer
	pending  []*PeerState          // pre-handshake peers
	onHello  func(*PeerState)      // called after successful hello handshake
}

// NewHub creates a new peer hub.
func NewHub(onHello func(*PeerState)) *Hub {
	return &Hub{
		peers:   make(map[string]*PeerState),
		onHello: onHello,
	}
}

// Get returns a peer by name.
func (h *Hub) Get(name string) (*PeerState, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, ok := h.peers[name]
	return p, ok
}

// List returns all connected peers.
func (h *Hub) List() []*PeerState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*PeerState, 0, len(h.peers))
	for _, p := range h.peers {
		out = append(out, p)
	}
	return out
}

// Broadcast sends a message to all connected peers.
func (h *Hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, p := range h.peers {
		_ = p.Send(data)
	}
}

// Remove disconnects and removes a peer from the hub.
func (h *Hub) Remove(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p, ok := h.peers[name]; ok {
		p.Close()
		delete(h.peers, name)
		slog.Info("peer removed", "name", name)
	}
}

func (h *Hub) registerPending(peer *PeerState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pending = append(h.pending, peer)
}

// PromotePending moves a peer from pending to registered after hello handshake.
// Returns true if the peer was found and promoted.
func (h *Hub) promotePending(conn *websocket.Conn, name string) *PeerState {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, p := range h.pending {
		if p.conn == conn {
			p.Name = name
			h.peers[name] = p
			h.pending = append(h.pending[:i], h.pending[i+1:]...)
			slog.Info("peer connected", "name", name, "addr", p.Addr)
			return p
		}
	}
	return nil
}

// Count returns the number of connected peers.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.peers)
}
