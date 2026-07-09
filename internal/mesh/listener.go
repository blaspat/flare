package mesh

import (
	"context"
	"fmt"
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
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("panic in connection handler", "remote", r.RemoteAddr, "recover", rec)
		}
	}()
	conn, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("websocket upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}

	l.onConn(conn)
}

// MessageHandler processes an incoming message from a peer.
type MessageHandler func(msg *Message, peer *PeerState)

// Hub manages all active peer connections.
type Hub struct {
	mu              sync.RWMutex
	peers           map[string]*PeerState // name -> peer
	pending         []*PeerState          // pre-handshake peers
	onHello         func(*PeerState)      // called after successful hello handshake
	reconnectMgr    *ReconnectManager
	typeHandlers    map[string]MessageHandler // registered per-type handlers
	peerChangeFn    func()                     // called when peer set changes
	peerConnectedFn func(name string)          // called when a peer is added
}

// NewHub creates a new peer hub.
func NewHub(onHello func(*PeerState)) *Hub {
	return &Hub{
		peers:        make(map[string]*PeerState),
		onHello:      onHello,
		typeHandlers: make(map[string]MessageHandler),
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

// SendTo sends a message to a specific peer by name.
// Returns an error if the peer is not found.
func (h *Hub) SendTo(name string, data []byte) error {
	h.mu.RLock()
	p, ok := h.peers[name]
	h.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %q not connected", name)
	}
	return p.Send(data)
}

// Remove disconnects and removes a peer from the hub.
// This is an intentional removal — no reconnect is triggered.
func (h *Hub) Remove(name string) {
	h.mu.Lock()
	if p, ok := h.peers[name]; ok {
		p.SetOnDisconnect(nil) // suppress reconnect — intentional removal
		p.Close()
		delete(h.peers, name)
		slog.Info("peer removed", "name", name)
		if rm := h.reconnectMgr; rm != nil {
			rm.Forget(name)
		}
		h.mu.Unlock()
		h.notifyPeerChange()
		return
	}
	h.mu.Unlock()
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

// SetReconnectManager sets the reconnect manager for automatic reconnection.
func (h *Hub) SetReconnectManager(rm *ReconnectManager) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reconnectMgr = rm
}

// AddPeer registers a peer connection in the hub, replacing any existing
// connection with the same name. For outgoing connections (addr != ""),
// the peer is tracked for automatic reconnection on drop.
// The disconnect handler is set up to remove the peer from the hub on
// unexpected disconnection and notify the reconnect manager.
func (h *Hub) AddPeer(name string, peer *PeerState, addr string) {
	h.mu.Lock()
	// Close existing connection with same name if any
	if existing, ok := h.peers[name]; ok {
		existing.SetOnDisconnect(nil) // suppress handler — we're replacing it
		existing.Close()
	}

	// Set up disconnect handler
	peer.SetOnDisconnect(func(n string) {
		h.mu.Lock()
		// Only remove if this exact peer is still registered
		if current, ok := h.peers[n]; ok && current == peer {
			delete(h.peers, n)
			h.mu.Unlock()
			h.notifyPeerChange()
		} else {
			h.mu.Unlock()
		}
		rm := h.reconnectMgr
		if rm != nil {
			rm.OnDisconnect(n)
		}
	})

	h.peers[name] = peer
	h.mu.Unlock()

	// Notify peer change watchers
	h.notifyPeerChange()

	// Notify peer-connected callback (for sync index exchange, etc.)
	if fn := h.peerConnectedFn; fn != nil {
		fn(name)
	}

	// Track for reconnect if this is an outgoing connection (has an address)
	if addr != "" {
		if rm := h.reconnectMgr; rm != nil {
			rm.Track(name, addr)
		}
	}
}

// Count returns the number of connected peers.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.peers)
}

// ListNames returns the names of all connected peers.
func (h *Hub) ListNames() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.peers))
	for name := range h.peers {
		out = append(out, name)
	}
	return out
}

// OnPeerChange registers a callback that fires when the peer set changes
// (a peer is added or removed).
func (h *Hub) OnPeerChange(fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peerChangeFn = fn
}

// notifyPeerChange fires the peer change callback, if set.
func (h *Hub) notifyPeerChange() {
	if h.peerChangeFn != nil {
		h.peerChangeFn()
	}
}

// OnPeerConnected registers a callback that fires when a peer is
// successfully added. The callback receives the peer's name.
func (h *Hub) OnPeerConnected(fn func(name string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peerConnectedFn = fn
}

// HandleMessageType registers a handler for a specific message type.
// When a peer receives a message of this type, the handler is called
// instead of the default switch in handleMessage.
func (h *Hub) HandleMessageType(msgType string, handler MessageHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.typeHandlers[msgType] = handler
}

// getMessageHandler returns the registered handler for a message type, or nil.
// Must be called with at least a read lock held, or after the hub is fully
// initialised and never modified (single-writer pattern).
func (h *Hub) getMessageHandler(msgType string) MessageHandler {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.typeHandlers[msgType]
}
