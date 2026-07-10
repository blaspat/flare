# Flare — Build Progress

## Phase 1: Skeleton ✅
- [x] Project structure & Go module
- [x] TOML config types & loading
- [x] CLI entry point (start, join, status commands)
- [x] Graceful shutdown (sigint context)

## Phase 2: P2P Mesh ✅
- [x] WebSocket peer listener & connection (incoming + outgoing)
- [x] Heartbeat & liveness tracking (15s interval, 45s timeout)
- [x] Message protocol framing (JSON: type, from, sent_at, payload)
- [x] Peer discovery (mDNS / static list)
- [x] Peer reconnect on drop

## Phase 3: File Sync ✅
- [x] File tracking (path, mtime, hash, version)
- [x] Causal timestamp ordering (vector clocks)
- [x] File transfer (chunked, resume)
- [x] Apply incoming changes

## Phase 4: Distributed Cron ✅
- [x] In-memory scheduler
- [x] Leader election (simplest-first)
- [x] Job handoff on node drop
- [x] Script execution with timeout

## Phase 5: Integration & Polish ✅
- [x] End-to-end test with 2 nodes
- [x] CLI polish (colors, progress bars)
- [x] Error handling hardening
- [x] Cross-platform build script
- [x] README.md

## Phase 6: Quality-of-Life Features

### P1 — Conflict Handling ✅
- [x] Rename conflicting files instead of silent overwrite
- [x] `.conflict.<node>.<timestamp>` suffix on conflicts
- [x] Report conflicts in CLI status

### P2 — Event-Driven File Watcher
- [ ] inotify on Linux, kqueue on macOS
- [ ] Fall back to polling when not available
- [ ] Integration with FileTracker.Scan

### P3 — Content-Defined Chunking
- [ ] Rabin fingerprint / gear hash chunker
- [ ] Delta sync (only transfer changed blocks)
- [ ] Backward-compatible with current whole-file protocol

### P3 — Direct TLS
- [ ] `tls_cert` / `tls_key` config options
- [ ] Wrap WebSocket listener with crypto/tls
- [ ] Graceful fallback to plain WS
