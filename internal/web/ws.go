package web

import (
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsWriteWait      = 10 * time.Second
	wsPongWait       = 30 * time.Second
	wsPingPeriod     = (wsPongWait * 9) / 10
	wsPushInterval   = 3 * time.Second
	maxMessageSize   = 4096
)

// handleWS upgrades to WebSocket and starts per-client goroutines.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("web: ws upgrade", "err", err)
		return
	}

	s.wsClientsMu.Lock()
	s.wsClients[conn] = struct{}{}
	s.wsClientsMu.Unlock()

	slog.Debug("web: ws client connected", "remote", conn.RemoteAddr())

	// Read pump (just pings, discard messages)
	go s.wsReadPump(conn)

	// Write pump — pushes state snapshots every wsPushInterval
	s.wsWritePump(conn)
}

// wsReadPump reads pong responses and discards any client messages.
func (s *Server) wsReadPump(conn *websocket.Conn) {
	defer func() {
		s.wsClientsMu.Lock()
		delete(s.wsClients, conn)
		s.wsClientsMu.Unlock()
		conn.Close()
		slog.Debug("web: ws client disconnected", "remote", conn.RemoteAddr())
	}()

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Debug("web: ws read error", "err", err)
			}
			break
		}
	}
}

// wsWritePump pushes full-state snapshots to a single WS client.
func (s *Server) wsWritePump(conn *websocket.Conn) {
	ticker := time.NewTicker(wsPushInterval)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	// Send an initial snapshot immediately
	if err := s.wsSendSnapshot(conn); err != nil {
		return
	}

	for {
		select {
		case <-ticker.C:
			if err := s.wsSendSnapshot(conn); err != nil {
				return
			}
		case evt, ok := <-eventBus():
			if !ok {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := conn.WriteJSON(map[string]interface{}{
				"type": evt.Type,
				"data": evt.Data,
			}); err != nil {
				return
			}
		}
	}
}

// wsSendSnapshot sends a full state snapshot to a single client.
func (s *Server) wsSendSnapshot(conn *websocket.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return conn.WriteJSON(map[string]interface{}{
		"type": "state",
		"data": s.buildSnapshot(),
	})
}

// Global event push — called by CLI wiring when something interesting happens.
var (
	pushMu   sync.Mutex
	pushOnce sync.Once
)

// PushEvent sends an event to all connected WS clients (best-effort, non-blocking).
func PushEvent(typ string, data interface{}) {
	select {
	case eventBus() <- Event{Type: typ, Data: data}:
	default:
		// drop if channel full
	}
}
