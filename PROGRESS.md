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
- [x] Peer discovery (mDNS / static list) ✅
- [x] Peer reconnect on drop ✅ — reconnect.go: ReconnectManager with configurable interval, disconnect callback on PeerState, AddPeer method on Hub, wired into start/join CLI, 11 tests

## Phase 3: File Sync ✅
- [x] File tracking (path, mtime, hash, version) ✅ — `internal/sync/tracker.go` with FileTracker, Scan/Snapshot/Get, SHA-256 hashing, change detection (created/modified/deleted), version counter, 17 tests
- [x] Causal timestamp ordering (vector clocks) ✅ — `internal/sync/vectorclock.go` with VectorClock type, Increment/Get/Set/Merge/Compare/Copy/Equal, JSON serialization, full causal ordering semantics (HappenedBefore/Concurrent/HappenedAfter), 22 tests
- [x] File transfer (chunked, resume) ✅ — `internal/sync/transfer.go`: ChunkFile splits files into configurable-size chunks (base64 over WebSocket), TransferManager handles send/receive with out-of-order chunk assembly, IncomingTransferStore tracks partial transfers, resume via missing-chunk re-request (FileResumeRequest), hash verification before apply, stale transfer cleanup, 17 tests
- [x] Apply incoming changes ✅ — TransferManager.HandleFileChange/HandleFileChunk/finalizeTransfer writes assembled files to correct watch-dir paths, verifies full-file SHA-256, handles file deletion announcements (Size=-1), registered via Hub.HandleMessageType for msgFileChange/msgFileChunk/msgFileResume

## Phase 4: Distributed Cron ✅
- [x] In-memory scheduler ✅ — `internal/cron/` package
- [x] Leader election (simplest-first) ✅ — `internal/election/` package: Elector type with lowest-name algorithm, Hub integration (OnPeerChange/ListNames/notifyPeerChange), wired into start/join CLI, 14 tests
- [x] Job handoff on node drop ✅ — `internal/cron/manager.go`: CronManager wraps Scheduler, starts/stops on leadership transitions via OnLeadershipChange, jobs handed off to new leader when current leader drops, 12 tests
- [x] Script execution with timeout ✅ — via exec.CommandContext in cron event handler, respects Job.Timeout, logs output/errors, wired into start/join CLI

## Current: Phase 5 — Integration & Polish
- [x] End-to-end test with 2 nodes ✅
- [x] CLI polish (colors, progress bars) ✅ — `internal/term/` package with ANSI colors, colorized help/status/banner, auto TTY detection, ProgressBar type, `NO_COLOR` support
- [x] Error handling hardening ✅ — hashFile uses tracker's hashFunc option, Marshal errors handled in Connect/StartListener, flag parse errors checked, runCmd implemented, unused ensureDir removed, panic recovery in listener handler
- [x] Cross-platform build script ✅ — `scripts/build.sh` builds linux/amd64 + linux/arm64 with ldflags versioning, optional UPX compression, checksums
- [x] README.md ✅ — install, config, usage, architecture overview, project structure

## Done
- [ ] Final handoff to Patrick (message + merge to master + push)
