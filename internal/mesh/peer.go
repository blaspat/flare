package mesh

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// PeerState tracks a single remote peer connection.
type PeerState struct {
	Name      string
	Addr      string
	Connected time.Time
	LastHeard time.Time
	LostCount int // consecutive missed heartbeats

	conn          *websocket.Conn
	mu            sync.Mutex
	done          chan struct{}
	seq           atomic.Uint64
	outbox        chan []byte
	onDisconnect  atomic.Pointer[func(name string)]
}

const (
	heartbeatInterval = 15 * time.Second
	heartbeatTimeout  = 45 * time.Second // after which peer is considered lost
	outboxSize        = 256
	writeTimeout      = 10 * time.Second
	readLimit         = 1 << 20 // 1 MB max message size
)

// NewPeer creates a new peer from an established WebSocket connection.
func NewPeer(name string, conn *websocket.Conn) *PeerState {
	conn.SetReadLimit(readLimit)
	return &PeerState{
		Name:      name,
		Addr:      conn.RemoteAddr().String(),
		Connected: time.Now(),
		LastHeard: time.Now(),
		conn:      conn,
		done:      make(chan struct{}),
		outbox:    make(chan []byte, outboxSize),
	}
}

// Start begins the read/write pump goroutines for this peer.
func (p *PeerState) Start(ctx context.Context, handler func(msg *Message)) {
	go p.writePump(ctx)
	go p.readPump(ctx, handler)
	go p.heartbeatLoop(ctx)
}

// Send queues a message for delivery to this peer.
func (p *PeerState) Send(msg []byte) error {
	select {
	case p.outbox <- msg:
		return nil
	default:
		return fmt.Errorf("peer %s outbox full", p.Name)
	}
}

// SetOnDisconnect registers a callback that fires on unexpected disconnection.
// Pass nil to clear. The callback is called at most once.
func (p *PeerState) SetOnDisconnect(fn func(name string)) {
	if fn == nil {
		p.onDisconnect.Store(nil)
	} else {
		p.onDisconnect.Store(&fn)
	}
}

// Close cleanly shuts down the peer connection.
// If a disconnect handler is set and Close is called by the read pump or
// heartbeat loop (unexpected drop), the handler fires once.
func (p *PeerState) Close() error {
	select {
	case <-p.done:
		return nil
	default:
		close(p.done)
	}
	slog.Debug("closing peer", "name", p.Name, "addr", p.Addr)
	err := p.conn.Close()

	if pfn := p.onDisconnect.Swap(nil); pfn != nil && *pfn != nil {
		(*pfn)(p.Name)
	}

	return err
}

// IsAlive returns true if the peer has been heard from recently.
func (p *PeerState) IsAlive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Since(p.LastHeard) < heartbeatTimeout
}

func (p *PeerState) writePump(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case msg, ok := <-p.outbox:
			if !ok {
				return
			}
			p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				slog.Warn("write to peer failed", "name", p.Name, "err", err)
				return
			}
		case <-ticker.C:
			// heartbeat is sent via the outbox, but this ticker
			// also serves as a write-liveness check
		}
	}
}

func (p *PeerState) readPump(ctx context.Context, handler func(msg *Message)) {
	defer p.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		default:
		}

		_, data, err := p.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("peer read error", "name", p.Name, "err", err)
			}
			return
		}

		p.mu.Lock()
		p.LastHeard = time.Now()
		p.mu.Unlock()

		msg, err := UnmarshalMessage(data)
		if err != nil {
			slog.Warn("invalid message from peer", "name", p.Name, "err", err)
			continue
		}

		handler(msg)
	}
}

func (p *PeerState) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			seq := p.seq.Add(1)
			msg := MustNewMessage(MsgHeartbeat, p.Name, &HeartbeatPayload{Seq: seq})
			data, _ := msg.Marshal()
			_ = p.Send(data)

			p.mu.Lock()
			stale := time.Since(p.LastHeard) > heartbeatTimeout
			p.mu.Unlock()

			if stale {
				slog.Warn("peer heartbeat timeout", "name", p.Name,
					"last_seen", p.LastHeard)
				p.Close()
				return
			}
		}
	}
}
