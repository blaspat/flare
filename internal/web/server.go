// Package web provides an embedded web dashboard for Flare — REST API +
// WebSocket real-time push, served on a configurable port alongside the mesh.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/blaspat/flare/internal/config"
	"github.com/blaspat/flare/internal/cron"
	"github.com/blaspat/flare/internal/mesh"
	flaresync "github.com/blaspat/flare/internal/sync"
	"github.com/gorilla/websocket"
)

//go:embed static/*
var staticFS embed.FS

// Global event bus: components (or internal goroutines) push Events here,
// and every connected WS client receives them.
var (
	globalEvents chan Event
	once         sync.Once
)

func eventBus() chan Event {
	once.Do(func() { globalEvents = make(chan Event, 128) })
	return globalEvents
}

// Event is pushed to WS clients in real-time.
type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// Server is the embedded web dashboard server.
type Server struct {
	hub  *mesh.Hub
	tm   *flaresync.TransferManager
	cm   *cron.Manager
	cfg  *config.Config
	auth *Auth

	nodeName  string
	startTime time.Time

	httpServer *http.Server
	upgrader   websocket.Upgrader

	wsClients   map[*websocket.Conn]struct{}
	wsClientsMu sync.Mutex
}

// New creates a web dashboard Server. Pass nil for any dependency to skip
// its related endpoints (safe to use before the full mesh is initialised).
func New(hub *mesh.Hub, tm *flaresync.TransferManager, cm *cron.Manager, cfg *config.Config, nodeName string) *Server {
	return &Server{
		hub:       hub,
		tm:        tm,
		cm:        cm,
		cfg:       cfg,
		auth:      NewAuth(cfg.Node.WebUsername, cfg.Node.WebPassword),
		nodeName:  nodeName,
		startTime: time.Now(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		wsClients: make(map[*websocket.Conn]struct{}),
	}
}

// Start begins serving the dashboard HTTP server on the configured port.
// Blocks until ctx is cancelled. Returns immediately if port <= 0.
func (s *Server) Start(ctx context.Context, port int) error {
	if port <= 0 {
		return nil
	}

	mux := http.NewServeMux()

	// Auth (login/logout) — no middleware required
	mux.HandleFunc("/api/login", s.auth.Login)
	mux.HandleFunc("/api/logout", s.auth.Logout)

	// Static login page
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		loginHTML, _ := staticFS.ReadFile("static/login.html")
		if loginHTML == nil {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(loginHTML)
	})

	// REST API
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/peers", s.handlePeers)
	mux.HandleFunc("/api/sync", s.handleSync)
	mux.HandleFunc("/api/cron", s.handleCron)

	// WebSocket push
	mux.HandleFunc("/api/ws", s.handleWS)

	// Static dashboard (SPA)
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return fmt.Errorf("web: fs sub static: %w", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	// Wrap with auth middleware
	handler := s.auth.Middleware(mux)

	// Start session cleanup if auth enabled
	if s.auth.Enabled() {
		go s.auth.cleanupExpired()
	}

	addr := fmt.Sprintf(":%d", port)
	slog.Info("web dashboard starting", "addr", addr, "port", port, "auth", s.auth.Enabled())
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down web dashboard")

		// Close all WS clients
		s.wsClientsMu.Lock()
		for conn := range s.wsClients {
			conn.Close()
		}
		s.wsClientsMu.Unlock()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// --- shared helpers ---------------------------------------------------------

// scheduleString returns the human-readable representation of a cron Schedule.
func scheduleString(s cron.Schedule) string {
	if s == nil {
		return ""
	}
	switch v := s.(type) {
	case *cron.EverySchedule:
		return "@every " + v.Interval.String()
	case *cron.CronSchedule:
		return "<cron>"
	default:
		return fmt.Sprintf("%T", s)
	}
}

// writeJSON is a helper for writing JSON responses.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("web: write json", "err", err)
	}
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
