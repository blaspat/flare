// Package sync provides file-change tracking and chunked transfer for the Flare
// edge-mesh file-sync subsystem.
package sync

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Helper: create a TransferManager for testing LWW merge logic -----------

func newTestMergeTM(t *testing.T) (*TransferManager, *FileTracker, string) {
	t.Helper()

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	watchDir := filepath.Join(tmpDir, "watch")
	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatalf("mkdir watch dir: %v", err)
	}

	dirs := []WatchDir{{Path: watchDir, Tag: "default"}}
	tracker := NewFileTracker(dirs)
	tracker.Scan() // initial scan to register the watch dir

	tm := NewTransferManager("test-node", dataDir, 65536, tracker, func(data []byte) {}, dirs, nil)
	return tm, tracker, watchDir
}

// createLocalFile creates a file in the watch dir and scans it into the tracker.
func createLocalFile(t *testing.T, watchDir, relPath, content string) {
	t.Helper()
	absPath := filepath.Join(watchDir, relPath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(absPath, []byte(content), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

// --- LWW: HandleFileChange tests -------------------------------------------

func TestLWW_AcceptWhenCausallyNewer(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "test.txt"
	createLocalFile(t, watchDir, relPath, "version 1 of file")

	// Scan to pick up the file
	tracker.Scan()

	// Manually set clock to simulate a "local" state: node-b version 1
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	localClock := map[string]uint64{"node-b": 1, "node-a": 0}
	tracker.SetFileClock(tf.Path, "node-b", localClock)

	// Incoming change with a clock that happened-after local
	// local: {node-a: 0, node-b: 1}
	// incoming: {node-a: 1, node-b: 1}  (node-a happened-after w.r.t. local since node-a:1 > node-a:0)
	incomingClock := map[string]uint64{"node-a": 1, "node-b": 1}
	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        "default",
		Size:       10,
		Hash:       "abc123",
		Version:    5,
		NodeID:     "node-a",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
		Clock:      incomingClock,
	}

	// This should be accepted (causally newer)
	tm.HandleFileChange("node-a", announce)

	existing := tm.incoming.Get(relPath, "node-a", 5)
	if existing == nil {
		t.Fatal("LWW: expected incoming transfer to be created (causally newer)")
	}
}

func TestLWW_RejectWhenCausallyOlder(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "test.txt"
	createLocalFile(t, watchDir, relPath, "version 2 of file")

	tracker.Scan()

	// Local state: node-b version 2 (causally newer)
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	localClock := map[string]uint64{"node-a": 0, "node-b": 2}
	tracker.SetFileClock(tf.Path, "node-b", localClock)

	// Incoming change with a clock that happened-before local
	// local: {node-a: 0, node-b: 2}
	// incoming: {node-a: 0, node-b: 1} (node-b:1 < node-b:2, so happened-before)
	incomingClock := map[string]uint64{"node-a": 0, "node-b": 1}
	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        "default",
		Size:       15,
		Hash:       "def456",
		Version:    3,
		NodeID:     "node-a",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
		Clock:      incomingClock,
	}

	tm.HandleFileChange("node-a", announce)

	existing := tm.incoming.Get(relPath, "node-a", 3)
	if existing != nil {
		t.Fatal("LWW: incoming transfer should NOT have been created (causally older)")
	}
}

func TestLWW_LowerNodeIDWinsOnConcurrent(t *testing.T) {
	t.Run("lower_node_wins", func(t *testing.T) {
		tm, tracker, watchDir := newTestMergeTM(t)
		relPath := "concurrent.txt"
		createLocalFile(t, watchDir, relPath, "local version")

		tracker.Scan()

		// Local state: node-b edited this file
		tf := tracker.GetByTagAndPath("default", relPath)
		if tf == nil {
			t.Fatalf("tracker should have the file after scan")
		}
		// Simulate: local file was last written by "node-b"
		localClock := map[string]uint64{"node-a": 1, "node-b": 1}
		tracker.SetFileClock(tf.Path, "node-b", localClock)

		// Incoming from "node-a" with same clock entries → concurrent
		// But "node-a" < "node-b" lexicographically, so node-a wins.
		incomingClock := map[string]uint64{"node-a": 1, "node-b": 1}
		announce := &FileChangeAnnounce{
			Path:       relPath,
			Tag:        "default",
			Size:       20,
			Hash:       "ghi789",
			Version:    7,
			NodeID:     "node-a",
			ChunkSize:  65536,
			ChunkCount: 1,
			ModTime:    time.Now().UnixNano(),
			Clock:      incomingClock,
		}

		tm.HandleFileChange("node-a", announce)

		existing := tm.incoming.Get(relPath, "node-a", 7)
		if existing == nil {
			t.Fatal("LWW: expected incoming transfer from node-a (lower node ID wins on concurrent)")
		}
	})

	t.Run("higher_node_loses", func(t *testing.T) {
		tm, tracker, watchDir := newTestMergeTM(t)
		relPath := "concurrent2.txt"
		createLocalFile(t, watchDir, relPath, "local version")

		tracker.Scan()

		// Local state: node-a edited this file (so local.LastWriter = "node-a")
		tf := tracker.GetByTagAndPath("default", relPath)
		if tf == nil {
			t.Fatalf("tracker should have the file after scan")
		}
		localClock := map[string]uint64{"node-a": 1, "node-b": 1}
		tracker.SetFileClock(tf.Path, "node-a", localClock)

		// Incoming from "node-b" with same clock entries → concurrent
		// "node-b" > "node-a", so node-b should lose.
		announceSameClock := map[string]uint64{"node-a": 1, "node-b": 1}
		announce := &FileChangeAnnounce{
			Path:       relPath,
			Tag:        "default",
			Size:       25,
			Hash:       "xyz999",
			Version:    9,
			NodeID:     "node-b",
			ChunkSize:  65536,
			ChunkCount: 1,
			ModTime:    time.Now().UnixNano(),
			Clock:      announceSameClock,
		}

		tm.HandleFileChange("node-b", announce)

		existing := tm.incoming.Get(relPath, "node-b", 9)
		if existing != nil {
			t.Fatal("LWW: incoming from node-b should be rejected (higher node ID loses on concurrent)")
		}
	})
}

func TestLWW_AcceptWhenLocalHashMatchesIncoming(t *testing.T) {
	// When hashes match, skip regardless of clock comparison.
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "samehash.txt"
	createLocalFile(t, watchDir, relPath, "same content")

	tracker.Scan()

	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	localClock := map[string]uint64{"node-a": 1, "node-b": 2}
	tracker.SetFileClock(tf.Path, "node-b", localClock)

	// Set the local hash to match what we'll put in the announce.
	// We need to read the actual hash.
	localFile := tracker.GetByTagAndPath("default", relPath)

	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        "default",
		Size:       localFile.Size,
		Hash:       localFile.Hash, // same hash
		Version:    10,
		NodeID:     "node-c",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
		Clock:      map[string]uint64{"node-a": 2, "node-b": 2, "node-c": 1},
	}

	tm.HandleFileChange("node-c", announce)

	existing := tm.incoming.Get(relPath, "node-c", 10)
	if existing != nil {
		t.Fatal("LWW: should NOT create transfer when hashes match (identical content)")
	}
}

func TestLWW_BackwardCompatNoIncomingClock(t *testing.T) {
	// When incoming announcement has no Clock, it should be accepted
	// (backward compatible with old clients).
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "noclocksend.txt"
	createLocalFile(t, watchDir, relPath, "local version")

	tracker.Scan()

	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	localClock := map[string]uint64{"node-a": 1}
	tracker.SetFileClock(tf.Path, "node-a", localClock)

	// Announce without Clock field (old client)
	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        "default",
		Size:       30,
		Hash:       "newhash001",
		Version:    11,
		NodeID:     "old-node",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
		// Clock is nil — old client
	}

	tm.HandleFileChange("old-node", announce)

	existing := tm.incoming.Get(relPath, "old-node", 11)
	if existing == nil {
		t.Fatal("LWW: expected incoming transfer when sender has no Clock (backward compat)")
	}
}

func TestLWW_BackwardCompatNoLocalClock(t *testing.T) {
	// When local file has no Clock, incoming with clock should be accepted.
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "nolocalfileclock.txt"
	createLocalFile(t, watchDir, relPath, "local version")

	tracker.Scan()

	// Local file has no Clock set — it was created by local scan only.

	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        "default",
		Size:       35,
		Hash:       "newhash002",
		Version:    12,
		NodeID:     "node-x",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
		Clock:      map[string]uint64{"node-x": 1},
	}

	tm.HandleFileChange("node-x", announce)

	existing := tm.incoming.Get(relPath, "node-x", 12)
	if existing == nil {
		t.Fatal("LWW: expected incoming transfer when local has no Clock (backward compat)")
	}
}

// --- LWW: Sync Index Reconciliation tests -----------------------------------

func TestLWW_SyncIndex_HappenedAfter(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-test.txt"
	createLocalFile(t, watchDir, relPath, "local version")

	tracker.Scan()

	// Set local clock: node-a is at version 1
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	tracker.SetFileClock(tf.Path, "node-a", map[string]uint64{"node-a": 1})

	// Peer's index: clock {node-a: 2} (happened-after local)
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    40,
				Hash:    "peer-hash-002",
				Version: 20,
				ModTime: time.Now().UnixNano(),
				NodeID:  "node-a",
				Clock:   map[string]uint64{"node-a": 2},
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	if requests == nil || len(requests.Files) == 0 {
		t.Fatal("LWW SyncIndex: expected to request file when peer is causally ahead")
	}
	if requests.Files[0].Path != relPath {
		t.Fatalf("expected request for %s, got %s", relPath, requests.Files[0].Path)
	}
}

func TestLWW_SyncIndex_HappenedBefore(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-old.txt"
	createLocalFile(t, watchDir, relPath, "local newer version")

	tracker.Scan()

	// Local is causally ahead: node-a at version 5
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	tracker.SetFileClock(tf.Path, "node-a", map[string]uint64{"node-a": 5})

	// Peer's index: clock {node-a: 3} (happened-before local)
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    50,
				Hash:    "peer-hash-003",
				Version: 15,
				ModTime: time.Now().UnixNano(),
				NodeID:  "node-b",
				Clock:   map[string]uint64{"node-a": 3},
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	if requests != nil {
		t.Fatal("LWW SyncIndex: should NOT request file when local is causally ahead")
	}
}

func TestLWW_SyncIndex_Concurrent_LowerNodeWins(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-concurrent.txt"
	createLocalFile(t, watchDir, relPath, "local version from node-a")

	tracker.Scan()

	// Local: last written by node-a (lower node ID)
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	tracker.SetFileClock(tf.Path, "node-a", map[string]uint64{"node-a": 1, "node-b": 1})

	// Peer's index: same clock (concurrent), from node-b (higher node ID)
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    60,
				Hash:    "peer-concurrent-hash",
				Version: 25,
				ModTime: time.Now().UnixNano(),
				NodeID:  "node-b",
				Clock:   map[string]uint64{"node-a": 1, "node-b": 1},
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	// node-a (local) < node-b (peer) → local wins → no request
	if requests != nil {
		t.Fatal("LWW SyncIndex: local node-a wins over node-b on concurrent, should not request")
	}
}

func TestLWW_SyncIndex_Concurrent_PeerLowerNodeWins(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-concurrent2.txt"
	createLocalFile(t, watchDir, relPath, "local version from node-z")

	tracker.Scan()

	// Local: last written by node-z (high node ID)
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	tracker.SetFileClock(tf.Path, "node-z", map[string]uint64{"node-a": 1, "node-z": 1})

	// Peer's index: same clock (concurrent), from node-a (lower node ID)
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    70,
				Hash:    "peer-concurrent-wins",
				Version: 30,
				ModTime: time.Now().UnixNano(),
				NodeID:  "node-a",
				Clock:   map[string]uint64{"node-a": 1, "node-z": 1},
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	// node-a (peer) < node-z (local) → peer wins → should request
	if requests == nil || len(requests.Files) == 0 {
		t.Fatal("LWW SyncIndex: expected request when peer (node-a) has lower node ID than local (node-z)")
	}
}

func TestLWW_SyncIndex_SameHashSkips(t *testing.T) {
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-samehash.txt"
	createLocalFile(t, watchDir, relPath, "content alpha")

	tracker.Scan()

	// Set the local file's hash to match what the peer claims.
	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}
	localClock := map[string]uint64{"node-a": 1}
	tracker.SetFileClock(tf.Path, "node-a", localClock)

	// Read the actual hash to use as the peer's hash.
	localFile := tracker.GetByTagAndPath("default", relPath)

	// Peer's index: causally ahead but same hash → should skip (content identical)
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    localFile.Size,
				Hash:    localFile.Hash, // same as local
				Version: 35,
				ModTime: time.Now().UnixNano(),
				NodeID:  "node-b",
				Clock:   map[string]uint64{"node-a": 2},
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	if requests != nil {
		t.Fatal("LWW SyncIndex: should NOT request when hashes match, even if causally ahead")
	}
}

func TestLWW_SyncIndex_FallbackVersion(t *testing.T) {
	// When both sides have no clock info, fall back to version+modtime comparison.
	tm, tracker, watchDir := newTestMergeTM(t)
	relPath := "sync-fallback.txt"
	createLocalFile(t, watchDir, relPath, "old version")

	tracker.Scan()

	// Local file has no clock info.
	// Local version is 1.

	// Peer's index: no clock info, but higher version number.
	index := &SyncIndexPayload{
		Files: []SyncIndexEntry{
			{
				Path:    relPath,
				Tag:     "default",
				Size:    80,
				Hash:    "fallback-peer-hash",
				Version: 100, // higher than local
				ModTime: time.Now().UnixNano(),
				// No NodeID, no Clock — old client
			},
		},
	}

	requests := tm.HandleSyncIndex("other-node", index)
	if requests == nil || len(requests.Files) == 0 {
		t.Fatal("LWW SyncIndex: expected request via fallback version comparison")
	}
}

// --- LWW: BuildSyncIndex tests ----------------------------------------------

func TestBuildSyncIndex_IncludesClock(t *testing.T) {
	_, tracker, watchDir := newTestMergeTM(t)
	relPath := "clock-index.txt"
	createLocalFile(t, watchDir, relPath, "tracked content")

	tracker.Scan()

	tf := tracker.GetByTagAndPath("default", relPath)
	if tf == nil {
		t.Fatalf("tracker should have the file after scan")
	}

	// Set clock info.
	clock := map[string]uint64{"node-a": 1, "node-b": 2}
	tracker.SetFileClock(tf.Path, "node-b", clock)

	// Create a minimal TM just to call BuildSyncIndex
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	bareTM := NewTransferManager("test-node", dataDir, 65536, tracker, func(data []byte) {}, []WatchDir{{Path: watchDir, Tag: "default"}}, nil)

	index := bareTM.BuildSyncIndex()
	if index == nil || len(index.Files) == 0 {
		t.Fatal("BuildSyncIndex should return files")
	}

	found := false
	for _, entry := range index.Files {
		if entry.Path == relPath {
			found = true
			if entry.Clock == nil {
				t.Fatal("BuildSyncIndex should include Clock in SyncIndexEntry")
			}
			if entry.NodeID != "node-b" {
				t.Fatalf("BuildSyncIndex NodeID: expected 'node-b', got '%s'", entry.NodeID)
			}
			if entry.Clock["node-a"] != 1 || entry.Clock["node-b"] != 2 {
				t.Fatalf("BuildSyncIndex Clock contents wrong: got %v", entry.Clock)
			}
		}
	}
	if !found {
		t.Fatalf("BuildSyncIndex missing file '%s'", relPath)
	}
}
