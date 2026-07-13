# Flare — Edge Mesh Server

<p align="center">
  <img src="assets/logo.svg" width="200" height="200" alt="Flare logo">
</p>

[![Go](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Flare is a single-binary edge mesh server written in Go. It connects machines
into a peer-to-peer mesh for **file synchronisation** and **distributed cron
job execution** — no central server, no database.

---

## Features

- **P2P mesh** — WebSocket peer connections with mDNS and static discovery,
  automatic reconnection, and direct NAT traversal
- **File sync** — Real-time event-driven change detection (SHA-256), chunked
  transfer with resume, vector clocks for causal ordering, content-defined
  chunking, and `.flareignore` support
- **Distributed cron** — Leader-elected (lowest-name) job scheduling with
  automatic handoff on node failure and catch-up on rejoin
- **Web dashboard** — Real-time status, peer list, sync activity, and cron
  history at `http://localhost:9722`
- **Setup wizard** — Browser-based first-run config (no terminal editing)
- **Encryption at rest** — Optional AES-256-GCM for synced files
- **Bandwidth throttling** — Rate-limit sync transfers per-node
- **Single binary** — Linux, macOS, Windows (amd64 + arm64). No runtime deps.
- **NSSM auto-install** — Windows service installs on double-click

---

## Quickstart

### Windows

```cmd
:: Download flare.exe, double-click it
:: Click Yes on the UAC prompt
:: Setup wizard opens at http://localhost:9722
:: Press any key to exit — Flare stays running
```

### Linux / macOS

```bash
# Download the binary, then:
./flare start

# Open http://localhost:9722 in your browser
# Fill in the setup wizard — no config file to touch
```

### Build from source

```bash
git clone https://github.com/blaspat/flare.git
cd flare
go build -o flare .
./flare start
```

---

## Install guides

| Platform | Guide |
|----------|-------|
| Linux | [LINUX.md](LINUX.md) — systemd, firewall, nginx proxy |
| macOS | [MACOS.md](MACOS.md) — LaunchAgent, Gatekeeper |
| Windows | [WINDOWS.md](WINDOWS.md) — double-click, NSSM service |

Pre-built binaries on the [releases page](https://github.com/blaspat/flare/releases).

---

## Usage (all platforms)

```
  flare start               Start the node
  flare start -d            Start in background (daemon, Linux/macOS)
  flare status              Show node and mesh status
  flare dashboard           Open the web dashboard in browser
  flare run <job-name>      Run a cron job immediately
  flare init                Generate a config file (terminal)

  Windows only:
  flare install             Install as a Windows service (NSSM)
  flare stop                Stop the Windows service
  flare uninstall           Remove the Windows service
```

### `flare start`

Starts the mesh node: listens for peers, connects to known peers, starts
file sync and cron. Opens the web dashboard on port 9722.

- **First run** — no config? The dashboard shows the **setup wizard** at
  `http://localhost:9722/setup`. Fill in the form, save, and you're done.
- **Linux/macOS** — blocks until SIGINT. Use `-d` to daemonize.
- **Windows** — auto-installs as a Windows service on first run, starts
  the service, and exits. The service keeps running in the background.
  Double-click `flare.exe` for the same flow.

```bash
# Start with debug logging
./flare start -v

# Start in background (Linux/macOS)
./flare start -d
```

### `flare status`

```bash
$ flare status
 Node:   node-alpha
  Listen: :9721
  Peers:  2 connected (2/2 alive)
    ● node-beta   — alive
    ● node-gamma  — alive
  Sync:   2 watch dir(s)
  Cron:   2 job(s)
```

### `flare dashboard`

Opens the web dashboard in your default browser. Point it at
`http://localhost:9722` for live status, sync activity, peer list, and cron
history.

### Other commands

- `flare join <addr>` — Connect to an existing mesh node
- `flare run <job-name>` — Execute a cron job immediately and print output
- `flare init` — Interactive terminal-based config generator (alternative
  to the web wizard)

---

## Configuration

Default config path: `./flare.toml` or `$FLARE_CONFIG`.

```toml
# flare.toml
[node]
name = "node-alpha"          # Unique name in the mesh
listen = ":9721"             # WebSocket listen address
data_dir = "./data"          # State and sync staging
log_level = "info"           # debug | info | warn | error
web_port = 9722              # Dashboard port (0 = disabled)

[mesh]
peers = [
  # "ws://node-beta.local:9721",
  # "ws://10.0.0.42:9721",
]
discovery = "mdns"           # "mdns" | "static"

[sync]
watch_dirs = [
  { path = "./shared", tag = "default" },
]
poll_interval = "5s"
chunk_size = 65536

[cron]
enabled = true
history_size = 100

[[cron.jobs]]
name = "disk-check"
schedule = "0 * * * *"
command = "df -h /"
timeout = "30s"

[[cron.jobs]]
name = "heartbeat"
schedule = "@every 1m"
command = "echo alive"
timeout = "10s"
```

### Schedule syntax

- **Cron format:** `minute hour dom month dow` (5 fields)
- **@every:** `@every 30s`, `@every 5m`, `@every 1h`
- Supports `*`, `*/N` (step), `N-M` (range), `N,M,L` (list)

### Setup wizard

The fastest way to configure Flare: run `flare start`, open
`http://localhost:9722`, and fill in the form. It writes the config to disk
and redirects to the live dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     flare node                               │
│                                                              │
│  ┌──────────┐   ┌───────────┐   ┌──────────────────────┐    │
│  │  Config   │──▶│   Hub     │──▶│   Peer Connections   │    │
│  │  (TOML)   │   │ (mesh)    │   │   (WebSocket)        │    │
│  └──────────┘   └─────┬─────┘   └──────────────────────┘    │
│                        │                                      │
│             ┌──────────┼──────────┐                           │
│             ▼          ▼          ▼                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                      │
│  │  File    │ │  Cron    │ │Election  │                      │
│  │  Sync    │ │  Manager │ │(leader)  │                      │
│  │(tracker, │ │(distrib.)│ │(lowest   │                      │
│  │transfer) │ │          │ │  name)   │                      │
│  └──────────┘ └──────────┘ └──────────┘                      │
│                                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                     │
│  │  mDNS    │ │Reconnect │ │  Web     │                     │
│  │Discovery │ │ Manager  │ │Dashboard │                     │
│  └──────────┘ └──────────┘ └──────────┘                     │
└─────────────────────────────────────────────────────────────┘
```

### Key concepts

**Mesh** — Every node connects to every peer via WebSockets. No central
coordinator. Messages are framed JSON with type routing.

**Leader election** — The connected node with the lowest name is the leader.
Every node independently computes the same result (no coordination messages).
When the leader disconnects, the next-lowest node takes over.

**File sync** — Changes are detected via event-driven filesystem watchers
(with polling fallback). Changed files are announced to all peers and
transferred in chunks with SHA-256 integrity verification. Vector clocks
provide causal ordering for concurrent edits.

**Cron execution** — Only the leader executes cron jobs. Scheduler hands off
to the new leader on leadership change. Optional catch-up runs missed jobs
after re-election.

**Setup wizard** — When no config exists, the dashboard server shows a
first-run setup form at `/setup`. Fill in node name, peers, watch dirs,
and save. No terminal editing required.

---

## Project structure

```
flare/
├── main.go                       # Entry point
├── internal/
│   ├── cmd/
│   │   ├── cli.go               # CLI commands (start, join, status, run)
│   │   ├── install.go           # Windows NSSM service management
│   │   ├── syscall_windows.go   # UAC elevation, ShellExecute
│   │   └── stubs_unix.go        # Non-Windows stubs
│   ├── config/config.go         # TOML config loading
│   ├── cron/
│   │   ├── schedule.go          # Cron expression parser
│   │   ├── scheduler.go         # In-memory job scheduler
│   │   ├── manager.go           # Leadership-aware lifecycle manager
│   │   └── history.go           # Job result history
│   ├── election/elector.go      # Lowest-name leader election
│   ├── mesh/
│   │   ├── peer.go              # Peer state, read/write pumps, heartbeat
│   │   ├── listener.go          # WebSocket listener
│   │   ├── mesh.go              # Hub, message routing, connect
│   │   ├── protocol.go          # Message framing (JSON)
│   │   ├── discovery.go         # mDNS discovery + static peers
│   │   └── reconnect.go         # Automatic reconnection
│   ├── sync/
│   │   ├── tracker.go           # File change detection
│   │   ├── transfer.go          # Chunked transfer + resume
│   │   ├── vectorclock.go       # Causal ordering
│   │   ├── watcher.go           # Event-driven fsnotify watcher
│   │   ├── ignore.go            # .flareignore patterns
│   │   ├── throttle.go          # Bandwidth throttling
│   │   ├── crypt.go             # AES-256-GCM encryption
│   │   └── gearhash.go          # Content-defined chunking
│   ├── nat/
│   │   ├── stun.go              # STUN NAT detection
│   │   ├── turn.go              # TURN relay
│   │   └── ice.go               # ICE candidate gathering
│   ├── web/
│   │   ├── server.go            # HTTP server + routes
│   │   ├── auth.go              # Session-based login
│   │   ├── api.go               # REST endpoints
│   │   ├── ws.go                # Real-time WebSocket push
│   │   ├── setup.go             # Setup wizard handler
│   │   └── static/              # Embedded SPA (HTML, CSS, SVG)
│   └── term/color.go            # Terminal colour helpers
├── scripts/build.sh             # Cross-platform build script
├── config.example.toml          # Example configuration
├── AGENTS.md                    # Developer guide
├── PROGRESS.md                  # Build progress
├── WINDOWS.md                   # Windows guide
├── LINUX.md                     # Linux guide
├── MACOS.md                     # macOS guide
└── go.mod / go.sum              # Go module
```

---

## Requirements

- **Go 1.26+** (to build from source)
- **Runtime:** Linux (amd64/arm64), macOS (Intel/Apple Silicon), Windows
  (amd64/arm64)
- **Network:** TCP port for WebSocket (default 9721), optional UDP 5353 for
  mDNS discovery, optional STUN/TURN for NAT traversal

---

## Development

```bash
# Build
go build -o flare .

# Run tests
go test ./...

# Run with debug logging
./flare start -v

# Cross-platform build
./scripts/build.sh
```

---

## License

MIT
