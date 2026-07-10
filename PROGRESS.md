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

### P4 — Content-Defined Chunking ✅ (2026-07-10)
- [x] Gear hash / Rabin fingerprint chunker
- [x] Delta sync (only transfer changed blocks)
- [x] Backward-compatible with current whole-file protocol

### P4 — Encryption at Rest ✅ (2026-07-10)
- [x] AES-GCM encrypt synced files on disk
- [x] Key management via config/env (inline hex, key file, env var)
- [x] Transparent decrypt on read (ReadDecryptedWithFallback for legacy files)
- [x] Full test suite in crypt_test.go (23 tests)

### P5 — NAT Traversal ✅ (2026-07-10)
- [x] STUN client for public address discovery
- [x] TURN relay fallback
- [x] ICE-style connection negotiation

### P5 — Distributed Cron HA ✅ (2026-07-10)
- [x] Cron job history / audit log
- [x] Missed-job catch-up on leader election
- [x] Configurable job retry policy

### P5 — CRDT-Style Merge ✅ (2026-07-10)
- [x] Last-writer-wins for concurrent file edits
- [x] Version vector reconciliation
- [x] Merge conflict reporting

---

## Phase 7: Web Dashboard

### P1 — Dashboard Backend
- [x] `internal/web/server.go` — HTTP server, routes, embed.FS for static assets (2026-07-10)
- [x] `internal/web/api.go` — REST endpoints (status, peers, sync, cron) (2026-07-10)
- [x] `internal/web/ws.go` — WebSocket event push to browser clients (2026-07-10)
- [x] Wire into `main.go` + config (web_port) (2026-07-10)

### P2 — Dashboard Frontend
- [x] `index.html` — dashboard layout with cards, tables, event log (2026-07-10)
- [x] `app.js` — WebSocket client, real-time updates, UI rendering (embedded in index.html) (2026-07-10)
- [x] `style.css` — clean dark theme, responsive layout (embedded in index.html) (2026-07-10)

### P3 — Polish
- [ ] Loading/error/empty states
- [x] Auto-reconnect WebSocket on disconnect (2026-07-10)
- [ ] Sort/filter peers table
- [ ] CLI `flare dashboard` command to open browser
