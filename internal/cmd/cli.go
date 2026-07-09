package cmd

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blaspat/flare/internal/config"
	"github.com/blaspat/flare/internal/cron"
	"github.com/blaspat/flare/internal/election"
	"github.com/blaspat/flare/internal/mesh"
	flaresync "github.com/blaspat/flare/internal/sync"
	"github.com/blaspat/flare/internal/term"
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
	case "init":
		return initCmd(ctx, args[2:])
	case "help", "--help", "-h":
		return printUsage()
	default:
		return fmt.Errorf("unknown command: %s", sub)
	}
}

func printUsage() error {
	fmt.Print(term.Cyan + term.Bold + `
   __ _ _ __ ___ _ __ ___
  / _' | '__/ _ \ '__/ __|
 | (_| | | |  __/ |  \__ \
  \__,_|_|  \___|_|  |___/
   Edge Mesh Server` + term.Reset + term.Dim + `  v0.1.0` + term.Reset + `

` + term.Bold + `Usage:` + term.Reset + `
  ` + term.Green + `flare start` + term.Reset + `              Start the mesh node (server mode)
  ` + term.Green + `flare start -d` + term.Reset + `           Start in background (daemon)
  ` + term.Green + `flare join` + term.Reset + ` <addr>        Join an existing mesh at address
  ` + term.Green + `flare status` + term.Reset + `             Show node and mesh status
  ` + term.Green + `flare run` + term.Reset + ` <job-name>     Run a cron job immediately
  ` + term.Green + `flare init` + term.Reset + `               Generate a config file interactively
  ` + term.Green + `flare help` + term.Reset + `               Show this help

` + term.Dim + `Config: FLARE_CONFIG env or ./flare.toml` + term.Reset + `
`)
	return nil
}

func startCmd(ctx context.Context, cfgPath string, args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	verbose := fs.Bool("v", false, "verbose logging")
	daemon := fs.Bool("d", false, "run in background (daemon mode)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	if *daemon && os.Getenv("_FLARE_DAEMON") == "" {
		// Re-exec without the -d flag, detach from terminal
		childArgs := make([]string, 0, len(os.Args))
		for _, a := range os.Args[1:] { // skip binary path, we pass it explicitly
			if a != "-d" && a != "--daemon" {
				childArgs = append(childArgs, a)
			}
		}
		proc, err := os.StartProcess(os.Args[0], append([]string{os.Args[0]}, childArgs...), &os.ProcAttr{
			Env:   append(os.Environ(), "_FLARE_DAEMON=1"),
			Files: []*os.File{nil, nil, nil}, // detach stdin/stdout/stderr
		})
		if err != nil {
			return fmt.Errorf("daemonize: %w", err)
		}
		pid := proc.Pid
		if err := proc.Release(); err != nil {
			return fmt.Errorf("release: %w", err)
		}
		fmt.Printf("Flare started in background (PID %d)\n", pid)
		return nil
	}

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
	// Show startup banner
	fmt.Print(term.BannerASCII())

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
	daemon := fs.Bool("d", false, "run in background (daemon mode)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	if *daemon && os.Getenv("_FLARE_DAEMON") == "" {
		childArgs := make([]string, 0, len(os.Args))
		for _, a := range os.Args[1:] {
			if a != "-d" && a != "--daemon" {
				childArgs = append(childArgs, a)
			}
		}
		proc, err := os.StartProcess(os.Args[0], append([]string{os.Args[0]}, childArgs...), &os.ProcAttr{
			Env:   append(os.Environ(), "_FLARE_DAEMON=1"),
			Files: []*os.File{nil, nil, nil},
		})
		if err != nil {
			return fmt.Errorf("daemonize: %w", err)
		}
		pid := proc.Pid
		if err := proc.Release(); err != nil {
			return fmt.Errorf("release: %w", err)
		}
		fmt.Printf("Flare started in background (PID %d)\n", pid)
		return nil
	}

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
	// Show startup banner
	fmt.Print(term.BannerASCII())

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
	fmt.Printf("%s Node:   %s%s\n", term.Bold+term.Cyan, term.Reset, cfg.Node.Name)
	fmt.Printf("  %sListen: %s%s\n", term.Bold, term.Reset, cfg.Node.Listen)

	hubMu.RLock()
	h := hub
	hubMu.RUnlock()

	if h != nil {
		peers := h.List()
		alive := 0
		for _, p := range peers {
			if p.IsAlive() {
				alive++
			}
		}
		healthColor := term.Green
		if alive == 0 {
			healthColor = term.Red
		} else if alive < len(peers) {
			healthColor = term.Yellow
		}
		fmt.Printf("  %sPeers: %s%d connected (%s", term.Bold, term.Reset, h.Count(), healthColor)
		fmt.Printf("%d/%d alive%s)\n", alive, len(peers), term.Reset)
		for _, p := range peers {
			status := term.Green + "● alive" + term.Reset
			if !p.IsAlive() {
				status = term.Red + "○ dead" + term.Reset
			}
			fmt.Printf("    %s %s (%s) — %s\n", term.Bullet(), p.Name, p.Addr, status)
		}
	} else {
		fmt.Printf("  %sPeers: %s%d configured (node not running)\n", term.Bold, term.Reset, len(cfg.Mesh.Peers))
	}

	watchCount := len(cfg.Sync.WatchDirs)
	fmt.Printf("  %sSync:  %s%d watch dir(s)\n", term.Bold, term.Reset, watchCount)
	jobCount := len(cfg.Cron.Jobs)
	fmt.Printf("  %sCron:  %s%d job(s)\n", term.Bold, term.Reset, jobCount)
	return nil
}

func runCmd(ctx context.Context, cfgPath string, args []string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if len(cfg.Cron.Jobs) == 0 {
		return fmt.Errorf("no cron jobs configured in %s", cfgPath)
	}

	jobName := strings.Join(args, " ")
	if jobName == "" {
		fmt.Println("Available cron jobs:")
		for _, j := range cfg.Cron.Jobs {
			fmt.Printf("  • %s: %q (%s)\n", j.Name, j.Command, j.Schedule)
		}
		return fmt.Errorf("usage: flare run <job-name>")
	}

	// Find the named job and execute it once.
	var found *config.CronJob
	for _, j := range cfg.Cron.Jobs {
		if j.Name == jobName {
			found = &j
			break
		}
	}
	if found == nil {
		return fmt.Errorf("cron job %q not found", jobName)
	}

	ctx, cancel := context.WithTimeout(ctx, found.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", found.Command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("job %q timed out after %v", found.Name, found.Timeout)
		}
		return fmt.Errorf("job %q failed: %w\noutput: %s", found.Name, err, string(output))
	}
	fmt.Printf("%s\n", string(output))
	return nil
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

func initCmd(ctx context.Context, args []string) error {
	_ = ctx
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	output := fs.String("o", "flare.toml", "output path for config file")
	_ = fs.Parse(args)

	reader := bufio.NewReader(os.Stdin)
	hostname, _ := os.Hostname()

	fmt.Print(term.Cyan + term.Bold + "\n  ⚡ Flare Setup\n" + term.Reset + "\n"+
		term.Dim+"  Press Enter to accept defaults.\n\n"+term.Reset)

	// Node name
	fmt.Printf("  Node name"+term.Dim+" [%s]"+term.Reset+": ", hostname)
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)
	if name == "" {
		name = hostname
	}

	// Listen address
	fmt.Printf("  Listen address"+term.Dim+" [:9721]"+term.Reset+": ")
	listen, _ := reader.ReadString('\n')
	listen = strings.TrimSpace(listen)
	if listen == "" {
		listen = ":9721"
	}

	// Data directory
	homeDir, _ := os.UserHomeDir()
	defaultData := filepath.Join(homeDir, ".flare")
	fmt.Printf("  Data directory"+term.Dim+" [%s]"+term.Reset+": ", defaultData)
	dataDir, _ := reader.ReadString('\n')
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		dataDir = defaultData
	}

	// Peers
	fmt.Print("  Peer addresses (comma-separated)"+term.Dim+" []"+term.Reset+": ")
	peersInput, _ := reader.ReadString('\n')
	peersInput = strings.TrimSpace(peersInput)
	var peers []string
	if peersInput != "" {
		for _, p := range strings.Split(peersInput, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}

	// Sync directories
	defaultShared := filepath.Join(dataDir, "shared")
	fmt.Printf("  Sync directories (comma-separated)"+term.Dim+" [%s]"+term.Reset+": ", defaultShared)
	syncDirs, _ := reader.ReadString('\n')
	syncDirs = strings.TrimSpace(syncDirs)
	if syncDirs == "" {
		syncDirs = defaultShared
	}
	var watchDirs []config.WatchDir
	for _, d := range strings.Split(syncDirs, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			watchDirs = append(watchDirs, config.WatchDir{Path: d, Tag: "default"})
		}
	}

	// Cron jobs
	fmt.Print("  Add a cron job? (name:schedule:command)"+term.Dim+" []"+term.Reset+": ")
	cronInput, _ := reader.ReadString('\n')
	cronInput = strings.TrimSpace(cronInput)
	var cronJobs []config.CronJob
	if cronInput != "" {
		parts := strings.SplitN(cronInput, ":", 3)
		if len(parts) == 3 {
			cronJobs = append(cronJobs, config.CronJob{
				Name:     strings.TrimSpace(parts[0]),
				Schedule: strings.TrimSpace(parts[1]),
				Command:  strings.TrimSpace(parts[2]),
				Timeout:  30_000_000_000, // 30s default
			})
		}
	}

	// Build config
	cfg := &config.Config{
		Node: config.NodeConfig{
			Name:     name,
			Listen:   listen,
			DataDir:  dataDir,
			LogLevel: "info",
		},
		Mesh: config.MeshConfig{
			Peers:             peers,
			Discovery:         "static",
			ReconnectInterval: 10_000_000_000,
		},
		Sync: config.SyncConfig{
			WatchDirs:    watchDirs,
			PollInterval: 5_000_000_000,
			ChunkSize:    65536,
		},
		Cron: config.CronConfig{
			Enabled:     true,
			HistorySize: 100,
			Jobs:        cronJobs,
		},
	}

	// Write config
	var buf strings.Builder
	// We'll use the toml library to write, but since we don't have a marshaler,
	// let's write it manually — cleaner output.
	buf.WriteString("[node]\n")
	buf.WriteString(fmt.Sprintf("name = %q\n", cfg.Node.Name))
	buf.WriteString(fmt.Sprintf("listen = %q\n", cfg.Node.Listen))
	buf.WriteString(fmt.Sprintf("data_dir = %q\n", cfg.Node.DataDir))
	buf.WriteString(fmt.Sprintf("log_level = %q\n", cfg.Node.LogLevel))

	buf.WriteString("\n[mesh]\n")
	var peerList []string
	for _, p := range cfg.Mesh.Peers {
		peerList = append(peerList, fmt.Sprintf("%q", p))
	}
	buf.WriteString(fmt.Sprintf("peers = [%s]\n", strings.Join(peerList, ", ")))
	buf.WriteString(fmt.Sprintf("discovery = %q\n", cfg.Mesh.Discovery))
	buf.WriteString(fmt.Sprintf("reconnect_interval = %q\n", "10s"))

	buf.WriteString("\n[sync]\n")
	buf.WriteString("watch_dirs = [\n")
	for _, wd := range cfg.Sync.WatchDirs {
		buf.WriteString(fmt.Sprintf("  { path = %q, tag = %q },\n", wd.Path, wd.Tag))
	}
	buf.WriteString("]\n")
	buf.WriteString(fmt.Sprintf("poll_interval = %q\n", "5s"))
	buf.WriteString(fmt.Sprintf("chunk_size = %d\n", cfg.Sync.ChunkSize))

	buf.WriteString("\n[cron]\n")
	buf.WriteString(fmt.Sprintf("enabled = %v\n", cfg.Cron.Enabled))
	buf.WriteString(fmt.Sprintf("history_size = %d\n", cfg.Cron.HistorySize))

	for _, j := range cfg.Cron.Jobs {
		buf.WriteString(fmt.Sprintf("\n[[cron.jobs]]\n"))
		buf.WriteString(fmt.Sprintf("name = %q\n", j.Name))
		buf.WriteString(fmt.Sprintf("schedule = %q\n", j.Schedule))
		buf.WriteString(fmt.Sprintf("command = %q\n", j.Command))
		buf.WriteString(fmt.Sprintf("timeout = %q\n", strconv.Itoa(int(j.Timeout.Seconds()))+"s"))
	}

	if err := os.WriteFile(*output, []byte(buf.String()), 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	fmt.Println("")
	fmt.Print("  " + term.Green + "✓" + term.Reset + " Config written to " + term.Bold + *output + term.Reset)

	// Create data dirs
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "\n  "+term.Yellow+"!"+term.Reset+" Could not create data dir: %v", err)
	}
	for _, wd := range watchDirs {
		if err := os.MkdirAll(wd.Path, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "\n  "+term.Yellow+"!"+term.Reset+" Could not create %s: %v", wd.Path, err)
		}
	}

	// Offer to start
	fmt.Print("\n\n  " + term.Dim + "Run `FLARE_CONFIG=" + *output + " flare start` to start the node." + term.Reset + "\n\n")

	return nil
}
