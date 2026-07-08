# Flare — Edge Mesh Server

A single-binary edge mesh server written in Go.
Runs on Linux (arm64 + amd64). No external deps beyond stdlib + BurntSushi/toml.

## Build
```
go build -o flare .
./flare --help
```

## Development Quickstart
```
cp config.example.toml flare.toml
# edit flare.toml to set node name, peers, watch dirs
./flare start
```

## Conventions
- Standard Go project layout: `main.go`, `internal/` packages
- `slog` for logging (structured, no third-party logger)
- Context-aware everywhere (no global state)
- Test filenames: `*_test.go` alongside source
- Config loaded once at startup, passed as dependency

## Current Phase
Check PROGRESS.md for exact checklist status. Each work session:
1. Read PROGRESS.md to find current phase
2. Pick the first unchecked item in "Current" phase
3. Implement it with tests
4. Build and verify
5. Mark item done in PROGRESS.md
6. If all phases done → notify Patrick and cancel cron

## Style
- Prefer `err :=` over `if err != nil` nesting where clean
- Use `net/http` + `golang.org/x/net/websocket` or `gorilla/websocket` if needed
- Single-file packages unless they exceed ~300 lines
- No init() functions
- Prefix TODO with `TODO(flare):` for tracked items
