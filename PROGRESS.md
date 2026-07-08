# Flare — Build Progress

## Phase 1: Skeleton ✅
- [x] Project structure & Go module
- [x] TOML config types & loading
- [x] CLI entry point (start, join, status commands)
- [x] Graceful shutdown (sigint context)

## Current: Phase 2 — P2P Mesh
- [x] WebSocket peer listener & connection (incoming + outgoing)
- [x] Heartbeat & liveness tracking (15s interval, 45s timeout)
- [x] Message protocol framing (JSON: type, from, sent_at, payload)
- [ ] Peer discovery (mDNS / static list)
- [ ] Peer reconnect on drop

## Phase 3: File Sync
- [ ] File tracking (path, mtime, hash, version)
- [ ] Causal timestamp ordering (vector clocks)
- [ ] File transfer (chunked, resume)
- [ ] Apply incoming changes

## Phase 4: Distributed Cron
- [ ] In-memory scheduler
- [ ] Leader election (simplest-first)
- [ ] Job handoff on node drop
- [ ] Script execution with timeout

## Phase 5: Integration & Polish
- [ ] End-to-end test with 2 nodes
- [ ] CLI polish (colors, progress bars)
- [ ] Error handling hardening
- [ ] Cross-platform build script

## Done
- [ ] Final handoff to Patrick
