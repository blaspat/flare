# Flare — Edge Mesh Server

<p align="center">
  <img src="assets/logo.svg" width="200" height="200" alt="Flare logo">
</p>

[![Go](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Flare is a single-binary edge mesh server written in Go. It connects machines
into a peer-to-peer mesh for **file synchronisation** and **distributed cron
job execution** — no central server, no external dependencies beyond a
TOML config file.

## Features

- **P2P mesh** — WebSocket-based peer connections with mDNS discovery and
  automatic reconnection
- **File sync** — Real-time file change detection (SHA-256), chunked transfer
  with resume, vector clocks for causal ordering across the mesh
- **Distributed cron** — Leader-elected (lowest-name) job scheduling with
  automatic handoff on node failure
- **Single binary** — Builds for Linux amd64/arm64, macOS amd64/arm64, and Windows amd64/arm64. No runtime deps.
  Configure with one TOML file.

## Quickstart

```bash
# Build
go build -o flare .

# Interactive setup — no config file editing needed
./flare init

# Or use the example config directly
cp config.example.toml flare.toml
# Edit flare.toml — set node name, peers, watch dirs

# Start a node
./flare start

# On another machine, join the mesh
./flare join ws://node-alpha.local:9721
```

## Install

### From source

```bash
git clone https://github.com/blaspat/flare.git
cd flare
go build -o /usr/local/bin/flare .
```

### Cross-platform build

```bash
# Build for linux/amd64, linux/arm64, windows/amd64, windows/arm64
./scripts/build.sh

# Output in dist/flare-<version>/
```

### Windows

See **[WINDOWS.md](WINDOWS.md)** for a complete setup guide — download, config, service install with NSSM, troubleshooting.

Quick start:

```cmd
flare.exe start
```

### Pre-built binaries

Download the latest release from the [releases page](https://github.com/blaspat/flare/releases).

## Configuration

Flare uses a single TOML file. Default path: `./flare.toml` or `$FLARE_CONFIG`.

```toml
# flare.toml
[node]
name = "node-alpha"          # Unique name in the mesh
listen = ":9721"             # WebSocket listen address
data_dir = "/var/lib/flare"  # State and sync staging
log_level = "info"           # debug | info | warn | error

[mesh]
peers = [
  # "ws://node-beta.local:9721",
  # "ws://10.0.0.42:9721",
]
discovery = "mdns"           # "mdns" | "static" | "both"
reconnect_interval = "10s"   # Retry frequency on disconnect

[sync]
watch_dirs = [
  { path = "./shared", tag = "default" },
]
poll_interval = "5s"         # Filesystem scan interval
chunk_size = 65536           # Transfer chunk size in bytes

[cron]
enabled = true
history_size = 100

[[cron.jobs]]
name = "disk-check"
schedule = "0 * * * *"       # Every hour
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
- Supports `*` (all), `*/N` (step), `N-M` (range), `N,M,L` (list)

## Usage

```
  flare init                Generate a config file interactively
  flare start               Start the mesh node (server mode)
  flare start -d            Start in background (daemon)
  flare join <addr>         Join an existing mesh at address
  flare status              Show node and mesh status
  flare run <job-name>      Run a cron job immediately
  flare help                Show this help
```

### `flare init`

Walks you through node setup interactively — no manual TOML editing.
Prompts for node name, listen address, data directory, peer addresses,
sync directories, and optional cron jobs. Creates the config file and
data directories.

```bash
$ flare init
  ⚡ Flare Setup

  Press Enter to accept defaults.

  Node name [vpn-instance]:
  Listen address [:9721]:
  Data directory [/home/user/.flare]:
  Peer addresses (comma-separated) []: wss://flare.example.com/mesh
  Sync directories (comma-separated) [/home/user/.flare/shared]:
  Add a cron job? (name:schedule:command) []:

  ✓ Config written to flare.toml

  Run `FLARE_CONFIG=flare.toml flare start` to start the node.
```

### `flare start`

Starts the node in server mode: listens for WebSocket peers, connects to
static peers and discovered mDNS nodes, starts file sync and cron subsystems.
Blocks until SIGINT (or use `-d` to daemonize).

```bash
# Start with verbose debug logging
./flare start -v

# Start in background
./flare start -d
```

### `flare join <addr>`

Connects to an existing mesh node. The connecting node becomes a full peer:
it syncs files, participates in leader election, and executes cron jobs
when elected leader.

```bash
./flare join ws://10.0.0.42:9721
```

### `flare status`

Shows the node's current state: name, listen address, connected peers with
liveness status, watch directory count, and cron job count.

```bash
$ flare status
 Node:   node-alpha
  Listen: :9721
  Peers:  2 connected (2/2 alive)
    ● node-beta (ws://10.0.0.42:9721) — ● alive
    ● node-gamma (ws://10.0.0.99:9721) — ● alive
  Sync:   1 watch dir(s)
  Cron:   2 job(s)
```

### `flare run <job-name>`

Executes a named cron job immediately and prints its output. Useful for
testing and ad-hoc execution.

```bash
./flare run disk-check
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    flare node                            │
│                                                          │
│  ┌────────┐   ┌───────────┐   ┌──────────────────────┐  │
│  │ Config │──▶│   Hub     │──▶│   Peer Connections    │  │
│  │ (TOML) │   │ (mesh)    │   │   (WebSocket)         │  │
│  └────────┘   └─────┬─────┘   └──────────────────────┘  │
│                      │                                    │
│           ┌──────────┼──────────┐                         │
│           ▼          ▼          ▼                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                  │
│  │  File    │ │  Cron    │ │Election  │                  │
│  │  Sync    │ │  Manager │ │(leader)  │                  │
│  │(tracker, │ │(distrib.)│ │(lowest   │                  │
│  │transfer) │ │          │ │  name)   │                  │
│  └──────────┘ └──────────┘ └──────────┘                  │
│                                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                 │
│  │ mDNS     │ │Reconnect │ │Listener  │                 │
│  │Discovery │ │Manager   │ │(WS)      │                 │
│  └──────────┘ └──────────┘ └──────────┘                 │
└─────────────────────────────────────────────────────────┘
```

### Key concepts

**Mesh:** Every node connects to every other peer via WebSockets. No central
coordinator. Messages are framed JSON with type routing.

**Leader election:** The connected node with the lowest name is the leader.
Every node independently computes the same result — no coordination messages
needed. When the leader disconnects, the next-lowest node takes over
automatically.

**File sync:** Changes are detected by polling watched directories and
computing SHA-256 hashes. Changed files are announced to all peers and
transferred in chunks with integrity verification. Vector clocks provide
causal ordering for concurrent edits.

**Cron execution:** Only the leader executes cron jobs. When the leader
changes, the scheduler is handed off to the new leader. Jobs are defined
in the TOML config.

## Project structure

```
flare/
├── main.go                       # Entry point
├── internal/
│   ├── cmd/cli.go               # CLI commands (start, join, status, run)
│   ├── config/config.go         # TOML config loading
│   ├── cron/
│   │   ├── schedule.go          # Cron expression parser
│   │   ├── scheduler.go         # In-memory job scheduler
│   │   └── manager.go           # Leadership-aware lifecycle manager
│   ├── election/elector.go      # Lowest-name leader election
│   ├── mesh/
│   │   ├── peer.go              # Peer state, read/write pumps, heartbeat
│   │   ├── listener.go          # WebSocket listener + Hub
│   │   ├── mesh.go              # Connect, StartListener, message routing
│   │   ├── protocol.go          # Message framing (JSON)
│   │   ├── discovery.go         # mDNS discovery + static peers
│   │   └── reconnect.go         # Automatic peer reconnection
│   ├── sync/
│   │   ├── tracker.go           # File system scanning + change detection
│   │   ├── transfer.go          # Chunked file transfer + resume
│   │   └── vectorclock.go       # Causal ordering via vector clocks
│   └── term/color.go            # Terminal colour helpers
├── scripts/build.sh              # Cross-platform build script
├── config.example.toml           # Example configuration
├── AGENTS.md                     # Developer guide
├── PROGRESS.md                   # Build progress
└── go.mod / go.sum              # Go module
```

## Requirements

- **Go 1.26+** (to build from source)
- **Linux** arm64 or amd64 (runtime)
- **Network:** UDP port 5353 for mDNS, TCP port for WebSocket (configurable)

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

## License

MIT
