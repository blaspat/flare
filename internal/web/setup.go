package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/blaspat/flare/internal/config"
	"github.com/BurntSushi/toml"
)

// setupRequest is the JSON body from the setup wizard form.
type setupRequest struct {
	Name        string   `json:"name"`
	Listen      string   `json:"listen"`
	DataDir     string   `json:"data_dir"`
	WebPort     int      `json:"web_port"`
	Peers       []string `json:"peers"`
	WatchDirs   []string `json:"watch_dirs"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	LogLevel    string   `json:"log_level"`
}

// handleSetupForm serves the setup wizard HTML page.
func (s *Server) handleSetupForm(w http.ResponseWriter, r *http.Request) {
	setupHTML, err := staticFS.ReadFile("static/setup.html")
	if err != nil {
		http.Error(w, "setup page not found", 404)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(setupHTML)
}

// handleSetupSave accepts the setup form JSON and writes the config file.
func (s *Server) handleSetupSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method not allowed")
		return
	}

	var req setupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Validate required fields
	if req.Name == "" {
		writeError(w, 400, "node name is required")
		return
	}
	if req.Listen == "" {
		req.Listen = ":9721"
	}
	if req.WebPort <= 0 {
		req.WebPort = 9722
	}
	if req.DataDir == "" {
		req.DataDir = "./data"
	}
	if req.LogLevel == "" {
		req.LogLevel = "info"
	}

	// Build the config
	watchDirs := make([]config.WatchDir, len(req.WatchDirs))
	for i, d := range req.WatchDirs {
		watchDirs[i] = config.WatchDir{Path: d, Tag: fmt.Sprintf("dir%d", i+1)}
	}

	// Determine config path
	cfgPath := s.configPath
	if cfgPath == "" {
		cfgPath = "./flare.toml"
	}

	// Ensure data directory exists
	if !filepath.IsAbs(req.DataDir) {
		absDir := filepath.Join(filepath.Dir(cfgPath), req.DataDir)
		if err := os.MkdirAll(absDir, 0755); err != nil {
			slog.Warn("setup: mkdir data dir", "path", absDir, "err", err)
		}
	}

	cfg := &config.Config{
		Node: config.NodeConfig{
			Name:       req.Name,
			Listen:     req.Listen,
			DataDir:    req.DataDir,
			LogLevel:   req.LogLevel,
			WebPort:    req.WebPort,
			WebUsername: req.Username,
			WebPassword: req.Password,
		},
		Mesh: config.MeshConfig{
			Peers:     req.Peers,
			Discovery: "static",
		},
		Sync: config.SyncConfig{
			WatchDirs:   watchDirs,
			PollInterval: 5 * time.Second,
		},
		Cron: config.CronConfig{
			Enabled: true,
		},
	}

	// Write to disk
	f, err := os.Create(cfgPath)
	if err != nil {
		writeError(w, 500, fmt.Sprintf("cannot create config: %v", err))
		return
	}
	defer f.Close()

	if err := toml.NewEncoder(f).Encode(cfg); err != nil {
		writeError(w, 500, fmt.Sprintf("cannot write config: %v", err))
		return
	}

	slog.Info("setup: config saved", "path", cfgPath, "node", req.Name)
	writeJSON(w, 200, map[string]interface{}{
		"status": "ok",
		"path":   cfgPath,
		"node":   req.Name,
	})
}
