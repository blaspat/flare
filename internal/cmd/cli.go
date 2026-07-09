package cmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/blaspat/flare/internal/config"
	"github.com/blaspat/flare/internal/cron"
	"github.com/blaspat/flare/internal/election"
	"github.com/blaspat/flare/internal/mesh"
	flaresync "github.com/blaspat/flare/internal/sync"
)

var (
	hubMu sync.RWMutex
	hub   *mesh.Hub
)

type Command struct {
	Name    string
	Flags   *flag.FlagSet
	Run     func(ctx context.Context, cfg *config.Config, args []string) error
}

func ParseAndRun(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return printUsage()
	}

	cfgPath := os.Getenv("FLARE_CONFIG")
	if cfgPath == "" {
		cfgPath = "./flare.toml"
	}

	sub := args[1]
	switch sub {
	case "start":
		return startCmd(ctx, cfgPath, args[2:])
	case "join":
		return joinCmd(ctx, cfgPath, args[2:])
	case "status":
		return statusCmd(ctx, cfgPath, args[2:])
	case "run":
		return runCmd(ctx, cfgPath, args[2:])
	case "help", "--help", "-h":
		return printUsage()
	default:
		return fmt.Errorf("unknown command: %s", sub)
	}
}

func printUsage() error {
	fmt.Print(`Flare — Edge Mesh Server

Usage:
  flare start              Start the mesh node (server mode)
  flare join <addr>        Join an existing mesh at address
  flare status             Show node and mesh status
  flare run <job-name>     Run a cron job immediately
  flare help               Show this help

Config: FLARE_CONFIG env or ./flare.toml
`)
	return nil
}

func startCmd(ctx context.Context, cfgPath string, args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	verbose := fs.Bool("v", false, "verbose logging")
	_ = fs.Parse(args)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ResolvePaths(); err != nil {
		return fmt.Errorf("resolve paths: %w", err)
	}

	setLogLevel(cfg.Node.LogLevel, *verbose)

	// Create hub and expose for status command
	h := mesh.NewHub(func(p *mesh.PeerState) {})
	hubMu.Lock()
	hub = h
	hubMu.Unlock()

	// Convert config cron jobs to cron.Job entries.
	var cronJobs []cron.Job
	for _, j := range cfg.Cron.Jobs {
		sched, err := cron.ParseSchedule(j.Schedule)
		if err != nil {
			return fmt.Errorf("cron job %q: parse schedule: %w", j.Name, err)
		}
		cronJobs = append(cronJobs, cron.Job{
			Name:     j.Name,
			Command:  j.Command,
			Timeout:  j.Timeout,
			Schedule: sched,
		})
	}

	// Create cron manager for distributed job scheduling.
	// The handler executes scripts with timeout.
	cm := cron.NewManager(func(e cron.Event) {
		ctx, cancel := context.WithTimeout(context.Background(), e.Timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", e.Command)
		output, err := cmd.CombinedOutput()
		if err != nil {
			if ctx.Err() != nil {
				slog.Warn("cron job timed out", "name", e.Name, "timeout", e.Timeout)
			} else {
				slog.Error("cron job failed", "name", e.Name, "err", err, "output", string(output))
			}
			return
		}
		slog.Info("cron job completed", "name", e.Name, "output", string(output))
	}, 0)
	cm.SetJobs(cronJobs)
	cm.Start(ctx)
	defer cm.Stop()

	// Create leader elector (lowest-name wins).
	// When leadership changes, the cron manager starts/stops the scheduler,
	// handing off jobs to the new leader on node drop.
	elector := election.NewElector(cfg.Node.Name, func(isLeader bool) {
		cm.OnLeadershipChange(isLeader)
		if isLeader {
			slog.Info("elected as leader — will execute cron jobs", "job_count", len(cronJobs))
		} else {
			slog.Info("leader is a different node — standby mode")
		}
	})
	h.OnPeerChange(func() { elector.Elect(h.ListNames()) })
	elector.Elect(h.ListNames())

	// Create reconnect manager for automatic peer reconnection
	rm := mesh.NewReconnectManager(h, cfg.Node.Name, cfg.Mesh.ReconnectInterval)
	h.SetReconnectManager(rm)
	defer rm.Stop()

	slog.Info("starting flare node", "name", cfg.Node.Name, "listen", cfg.Node.Listen)

	// Start mesh listener
	_ = mesh.StartListener(ctx, cfg.Node.Listen, cfg.Node.Name, h)

	// Build watch directories for file sync.
	watchDirs := make([]flaresync.WatchDir, len(cfg.Sync.WatchDirs))
	for i, wd := range cfg.Sync.WatchDirs {
		watchDirs[i] = flaresync.WatchDir{Path: wd.Path, Tag: wd.Tag}
	}

	// Chunk size: default 64 KB.
	chunkSize := cfg.Sync.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 65536
	}

	// Create file tracker for sync.
	tracker := flaresync.NewFileTracker(watchDirs)

	// Create transfer manager.
	sm := flaresync.NewTransferManager(
		cfg.Node.Name,
		cfg.Node.DataDir,
		chunkSize,
		tracker,
		func(data []byte) { h.Broadcast(data) },
		watchDirs,
	)

	// Register sync message handlers with the hub.
	h.HandleMessageType(mesh.MsgFileChange, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileChangeAnnounce](msg)
		if err != nil {
			slog.Warn("decode file_change payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileChange(msg.From, payload)
	})
	h.HandleMessageType(mesh.MsgFileChunk, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileChunkPayload](msg)
		if err != nil {
			slog.Warn("decode file_chunk payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileChunk(msg.From, payload)
	})
	h.HandleMessageType(mesh.MsgFileResume, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileResumeRequest](msg)
		if err != nil {
			slog.Warn("decode file_resume payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileResume(msg.From, payload)
	})

	// Start sync polling loop.
	pollInterval := cfg.Sync.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	go func() {
		pollTicker := time.NewTicker(pollInterval)
		defer pollTicker.Stop()

		// Initial poll immediately.
		if err := sm.Poll(); err != nil {
			slog.Warn("initial sync poll", "err", err)
		}

		// Stale transfer cleanup loop (every 30s).
		cleanupTicker := time.NewTicker(30 * time.Second)
		defer cleanupTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
				if err := sm.Poll(); err != nil {
					slog.Warn("sync poll", "err", err)
				}
			case <-cleanupTicker.C:
				cleaned := sm.CleanStaleTransfers(5 * time.Minute)
				if cleaned > 0 {
					slog.Debug("cleaned stale transfers", "count", cleaned)
				}
			}
		}
	}()

	// Start peer discovery (connects to static peers + discovers via mDNS)
	go func() {
		if err := mesh.StartDiscovery(ctx, mesh.DiscoveryConfig{
			NodeName:    cfg.Node.Name,
			ListenAddr:  cfg.Node.Listen,
			StaticPeers: cfg.Mesh.Peers,
			Mode:        cfg.Mesh.Discovery,
		}, h); err != nil {
			slog.Warn("discovery stopped", "err", err)
		}
	}()

	// Block until signal
	<-ctx.Done()
	slog.Info("shutting down")
	return nil
}

func joinCmd(ctx context.Context, cfgPath string, args []string) error {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	verbose := fs.Bool("v", false, "verbose logging")
	_ = fs.Parse(args)

	if fs.NArg() == 0 {
		return fmt.Errorf("usage: flare join <ws-address>")
	}

	addr := fs.Arg(0)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ResolvePaths(); err != nil {
		return fmt.Errorf("resolve paths: %w", err)
	}

	setLogLevel(cfg.Node.LogLevel, *verbose)
	slog.Info("joining mesh", "name", cfg.Node.Name, "peer", addr)

	// Create hub and start listener
	h := mesh.NewHub(func(p *mesh.PeerState) {})
	hubMu.Lock()
	hub = h
	hubMu.Unlock()

	// Convert config cron jobs to cron.Job entries.
	var cronJobs []cron.Job
	for _, j := range cfg.Cron.Jobs {
		sched, err := cron.ParseSchedule(j.Schedule)
		if err != nil {
			return fmt.Errorf("cron job %q: parse schedule: %w", j.Name, err)
		}
		cronJobs = append(cronJobs, cron.Job{
			Name:     j.Name,
			Command:  j.Command,
			Timeout:  j.Timeout,
			Schedule: sched,
		})
	}

	// Create cron manager for distributed job scheduling.
	// The handler executes scripts with timeout.
	cm := cron.NewManager(func(e cron.Event) {
		ctx, cancel := context.WithTimeout(context.Background(), e.Timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", e.Command)
		output, err := cmd.CombinedOutput()
		if err != nil {
			if ctx.Err() != nil {
				slog.Warn("cron job timed out", "name", e.Name, "timeout", e.Timeout)
			} else {
				slog.Error("cron job failed", "name", e.Name, "err", err, "output", string(output))
			}
			return
		}
		slog.Info("cron job completed", "name", e.Name, "output", string(output))
	}, 0)
	cm.SetJobs(cronJobs)
	cm.Start(ctx)
	defer cm.Stop()

	// Create leader elector (lowest-name wins).
	// When leadership changes, the cron manager starts/stops the scheduler,
	// handing off jobs to the new leader on node drop.
	elector := election.NewElector(cfg.Node.Name, func(isLeader bool) {
		cm.OnLeadershipChange(isLeader)
		if isLeader {
			slog.Info("elected as leader — will execute cron jobs", "job_count", len(cronJobs))
		} else {
			slog.Info("leader is a different node — standby mode")
		}
	})
	h.OnPeerChange(func() { elector.Elect(h.ListNames()) })
	elector.Elect(h.ListNames())

	// Create reconnect manager for automatic peer reconnection
	rm := mesh.NewReconnectManager(h, cfg.Node.Name, cfg.Mesh.ReconnectInterval)
	h.SetReconnectManager(rm)
	defer rm.Stop()

	_ = mesh.StartListener(ctx, cfg.Node.Listen, cfg.Node.Name, h)

	// Build watch directories for file sync.
	watchDirs := make([]flaresync.WatchDir, len(cfg.Sync.WatchDirs))
	for i, wd := range cfg.Sync.WatchDirs {
		watchDirs[i] = flaresync.WatchDir{Path: wd.Path, Tag: wd.Tag}
	}

	// Chunk size: default 64 KB.
	chunkSize := cfg.Sync.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 65536
	}

	// Create file tracker and transfer manager.
	tracker := flaresync.NewFileTracker(watchDirs)
	sm := flaresync.NewTransferManager(
		cfg.Node.Name,
		cfg.Node.DataDir,
		chunkSize,
		tracker,
		func(data []byte) { h.Broadcast(data) },
		watchDirs,
	)

	// Register sync message handlers.
	h.HandleMessageType(mesh.MsgFileChange, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileChangeAnnounce](msg)
		if err != nil {
			slog.Warn("decode file_change payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileChange(msg.From, payload)
	})
	h.HandleMessageType(mesh.MsgFileChunk, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileChunkPayload](msg)
		if err != nil {
			slog.Warn("decode file_chunk payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileChunk(msg.From, payload)
	})
	h.HandleMessageType(mesh.MsgFileResume, func(msg *mesh.Message, peer *mesh.PeerState) {
		payload, err := mesh.DecodePayload[flaresync.FileResumeRequest](msg)
		if err != nil {
			slog.Warn("decode file_resume payload", "from", msg.From, "err", err)
			return
		}
		sm.HandleFileResume(msg.From, payload)
	})

	// Start sync polling loop.
	pollInterval := cfg.Sync.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	go func() {
		pollTicker := time.NewTicker(pollInterval)
		defer pollTicker.Stop()

		if err := sm.Poll(); err != nil {
			slog.Warn("initial sync poll", "err", err)
		}

		cleanupTicker := time.NewTicker(30 * time.Second)
		defer cleanupTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
				if err := sm.Poll(); err != nil {
					slog.Warn("sync poll", "err", err)
				}
			case <-cleanupTicker.C:
				cleaned := sm.CleanStaleTransfers(5 * time.Minute)
				if cleaned > 0 {
					slog.Debug("cleaned stale transfers", "count", cleaned)
				}
			}
		}
	}()

	// Connect to the specified peer
	peer, err := mesh.Connect(ctx, addr, cfg.Node.Name, h)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	slog.Info("joined mesh via peer", "peer", peer.Name, "addr", addr)

	<-ctx.Done()
	return nil
}

func statusCmd(ctx context.Context, cfgPath string, args []string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	fmt.Printf("Node:   %s\n", cfg.Node.Name)
	fmt.Printf("Listen: %s\n", cfg.Node.Listen)

	hubMu.RLock()
	h := hub
	hubMu.RUnlock()

	if h != nil {
		peers := h.List()
		fmt.Printf("Peers:  %d connected\n", h.Count())
		for _, p := range peers {
			fmt.Printf("  • %s (%s) — alive: %v\n", p.Name, p.Addr, p.IsAlive())
		}
	} else {
		fmt.Printf("Peers:  %d configured (node not running)\n", len(cfg.Mesh.Peers))
	}

	fmt.Printf("Sync:   %d watch dir(s)\n", len(cfg.Sync.WatchDirs))
	fmt.Printf("Cron:   %d job(s)\n", len(cfg.Cron.Jobs))
	return nil
}

func runCmd(ctx context.Context, cfgPath string, args []string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	_ = cfg
	return fmt.Errorf("not yet implemented")
}

func setLogLevel(level string, verbose bool) {
	var l slog.Leveler = slog.LevelInfo
	if verbose {
		l = slog.LevelDebug
	} else {
		switch level {
		case "debug":
			l = slog.LevelDebug
		case "warn":
			l = slog.LevelWarn
		case "error":
			l = slog.LevelError
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
	})))
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0755)
}
