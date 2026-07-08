package cmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/blaspat/flare/internal/config"
	"github.com/blaspat/flare/internal/mesh"
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

	slog.Info("starting flare node", "name", cfg.Node.Name, "listen", cfg.Node.Listen)

	// Start mesh listener
	_ = mesh.StartListener(ctx, cfg.Node.Listen, cfg.Node.Name, h)

	// Connect to configured peers
	for _, peerAddr := range cfg.Mesh.Peers {
		addr := peerAddr
		go func() {
			slog.Info("connecting to peer", "addr", addr)
			p, err := mesh.Connect(ctx, addr, cfg.Node.Name, h)
			if err != nil {
				slog.Warn("failed to connect to peer", "addr", addr, "err", err)
				return
			}
			_ = p
		}()
	}

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

	_ = mesh.StartListener(ctx, cfg.Node.Listen, cfg.Node.Name, h)

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
