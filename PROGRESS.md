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

### P1 — Conflict Handling ✅ (2026-07-10)
- [x] Rename conflicting files instead of silent overwrite
- [x] `.conflict.<node>.<timestamp>` suffix on conflicts
- [x] Report conflicts in CLI status

### P2 — Event-Driven File Watcher ✅ (2026-07-10)
- [x] inotify on Linux, kqueue on macOS
- [x] Fall back to polling when not available
- [x] Integration with FileTracker.Scan (via DirWatcher → TransferManager.Poll)

### P3 — Peer Discovery Backoff ✅ (2026-07-10)
- [x] Exponential backoff + jitter on reconnect
- [x] Configurable min/max delay
- [x] Circuit breaker after N consecutive failures

### P4 — Bandwidth Throttling ✅ (2026-07-10)
- [x] Token bucket rate limiter for transfers
- [x] Configurable bytes-per-second limit
- [x] Per-peer throttle configuration

### P4 — Direct TLS ✅ (2026-07-10)
- [x] `tls_cert` / `tls_key` config options
- [x] Wrap WebSocket listener with crypto/tls
- [x] Graceful fallback to plain WS

### P4 — Content-Defined Chunking
- [ ] Gear hash / Rabin fingerprint chunker
- [ ] Delta sync (only transfer changed blocks)
- [ ] Backward-compatible with current whole-file protocol

### P4 — Encryption at Rest
- [ ] AES-GCM encrypt synced files on disk
- [ ] Key management via config/env
- [ ] Transparent decrypt on read

### P5 — NAT Traversal
- [ ] STUN client for public address discovery
- [ ] TURN relay fallback
- [ ] ICE-style connection negotiation

### P5 — Distributed Cron HA
- [ ] Cron job history / audit log
- [ ] Missed-job catch-up on leader election
- [ ] Configurable job retry policy

### P5 — CRDT-Style Merge
- [ ] Last-writer-wins for concurrent file edits
- [ ] Version vector reconciliation
- [ ] Merge conflict reporting
