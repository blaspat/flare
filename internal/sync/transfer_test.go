package sync

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Test helpers ----------------------------------------------------------

// testContent returns a byte slice of the given length filled with a
// deterministic pattern so chunk boundaries are predictable.
func testContent(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// writeTestFile creates a temp file with the given content and returns its
// absolute path.
func writeTestFile(t *testing.T, dir string, name string, content []byte) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// hashOf returns the hex SHA-256 of data.
func hashOf(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// toB64 base64-encodes data for wire-format payloads.
func toB64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// nullBroadcast is a no-op broadcast function for tests.
func nullBroadcast(data []byte) {}

// --- ChunkFile tests -------------------------------------------------------

func TestChunkFile_ExactChunks(t *testing.T) {
	dir := t.TempDir()
	content := testContent(128 * 1024) // 128 KB
	path := writeTestFile(t, dir, "exact.dat", content)

	cf, err := ChunkFile(path, 64*1024, nil)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if cf.Size != 128*1024 {
		t.Errorf("want size %d, got %d", 128*1024, cf.Size)
	}
	if cf.Hash != hashOf(content) {
		t.Errorf("want hash %s, got %s", hashOf(content), cf.Hash)
	}
	if len(cf.Chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(cf.Chunks))
	}
	if cf.Chunks[0].Index != 0 || cf.Chunks[1].Index != 1 {
		t.Errorf("unexpected chunk indices: 0=%d, 1=%d", cf.Chunks[0].Index, cf.Chunks[1].Index)
	}
	if len(cf.Chunks[0].Data) != 64*1024 {
		t.Errorf("chunk 0 size: want %d, got %d", 64*1024, len(cf.Chunks[0].Data))
	}
	if len(cf.Chunks[1].Data) != 64*1024 {
		t.Errorf("chunk 1 size: want %d, got %d", 64*1024, len(cf.Chunks[1].Data))
	}
}

func TestChunkFile_PartialLastChunk(t *testing.T) {
	dir := t.TempDir()
	content := testContent(100000) // 100 KB, not evenly divisible by 64 KB
	path := writeTestFile(t, dir, "partial.dat", content)

	cf, err := ChunkFile(path, 64*1024, nil)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if cf.Size != 100000 {
		t.Errorf("want size %d, got %d", 100000, cf.Size)
	}
	if cf.Hash != hashOf(content) {
		t.Errorf("hash mismatch: want %s, got %s", hashOf(content), cf.Hash)
	}
	// 100000 / 65536 = 1 remainder 34464 → 2 chunks
	if len(cf.Chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(cf.Chunks))
	}
	if len(cf.Chunks[0].Data) != 65536 {
		t.Errorf("chunk 0 size: want 65536, got %d", len(cf.Chunks[0].Data))
	}
	if len(cf.Chunks[1].Data) != 34464 {
		t.Errorf("chunk 1 size: want 34464, got %d", len(cf.Chunks[1].Data))
	}
	if cf.Chunks[1].Hash != hashOf(content[65536:]) {
		t.Errorf("chunk 1 hash mismatch")
	}
}

func TestChunkFile_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := writeTestFile(t, dir, "empty.dat", []byte{})

	cf, err := ChunkFile(path, 64*1024, nil)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if cf.Size != 0 {
		t.Errorf("want size 0, got %d", cf.Size)
	}
	if len(cf.Chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(cf.Chunks))
	}
	if len(cf.Chunks[0].Data) != 0 {
		t.Errorf("chunk data: want empty, got %d bytes", len(cf.Chunks[0].Data))
	}
	if cf.Hash != hashOf([]byte{}) {
		t.Error("empty file hash mismatch")
	}
}

func TestChunkFile_SingleByte(t *testing.T) {
	dir := t.TempDir()
	content := []byte{'A'}
	path := writeTestFile(t, dir, "single.dat", content)

	cf, err := ChunkFile(path, 64*1024, nil)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(cf.Chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(cf.Chunks))
	}
	if len(cf.Chunks[0].Data) != 1 || cf.Chunks[0].Data[0] != 'A' {
		t.Error("chunk data mismatch")
	}
	if cf.Hash != hashOf(content) {
		t.Error("hash mismatch")
	}
}

func TestChunkFile_NotExists(t *testing.T) {
	_, err := ChunkFile("/nonexistent/path.dat", 64*1024, nil)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

// --- IncomingTransferStore tests -------------------------------------------

func TestIncomingTransferStore_CRUD(t *testing.T) {
	store := NewIncomingTransferStore()

	t1 := &IncomingTransfer{
		Path:    "dir/file.txt",
		NodeID:  "node-a",
		Version: 1,
	}
	t2 := &IncomingTransfer{
		Path:    "other.log",
		NodeID:  "node-b",
		Version: 3,
	}

	store.Set(t1)
	store.Set(t2)

	got := store.Get("dir/file.txt", "node-a", 1)
	if got == nil {
		t.Fatal("expected to find t1")
	}
	if got.Path != "dir/file.txt" {
		t.Errorf("want dir/file.txt, got %s", got.Path)
	}

	// Get unknown key.
	unknown := store.Get("nope", "node-a", 99)
	if unknown != nil {
		t.Errorf("expected nil for unknown key")
	}

	// List.
	list := store.List()
	if len(list) != 2 {
		t.Fatalf("want 2 entries, got %d", len(list))
	}

	// Remove.
	store.Remove("dir/file.txt", "node-a", 1)
	if store.Get("dir/file.txt", "node-a", 1) != nil {
		t.Error("expected nil after remove")
	}
}

// --- TransferManager tests -------------------------------------------------

func TestTransferManager_ResolveDestPath(t *testing.T) {
	tm := NewTransferManager("test-node", "/tmp/data", 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: "/watch/configs", Tag: "configs"},
			{Path: "/watch/data", Tag: "data"},
		}, nil)

	dest := tm.resolveDestPath("configs", "nginx/nginx.conf")
	if dest != "/watch/configs/nginx/nginx.conf" {
		t.Errorf("want /watch/configs/nginx/nginx.conf, got %s", dest)
	}

	dest = tm.resolveDestPath("data", "logs/app.log")
	if dest != "/watch/data/logs/app.log" {
		t.Errorf("want /watch/data/logs/app.log, got %s", dest)
	}

	// Unknown tag → falls back to data dir.
	dest = tm.resolveDestPath("unknown", "file.txt")
	if !strings.Contains(dest, "unknown/file.txt") {
		t.Errorf("expected fallback path containing unknown/file.txt, got %s", dest)
	}
}

func TestTransferManager_RelativePath(t *testing.T) {
	tm := NewTransferManager("test-node", "/tmp/data", 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: "/watch/configs", Tag: "configs"},
		}, nil)

	rel := tm.relativePath("/watch/configs/nginx/nginx.conf", "configs")
	if rel != "nginx/nginx.conf" {
		t.Errorf("want nginx/nginx.conf, got %s", rel)
	}
}

func TestTransferManager_HandleFileChangeAndChunk_Success(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")
	content := testContent(100000)

	// Path where the receiver should write the file.
	expectedPath := filepath.Join(watchDir, "received.dat")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Simulate receiving a FileChangeAnnounce.
	announce := &FileChangeAnnounce{
		Path:       "received.dat",
		Tag:        "default",
		Size:       int64(len(content)),
		Hash:       hashOf(content),
		Version:    1,
		NodeID:     "sender",
		ChunkSize:  65536,
		ChunkCount: 2,
		ModTime:    time.Now().UnixNano(),
	}
	tm.HandleFileChange("sender", announce)

	// Send chunks.
	chunk0 := &FileChunkPayload{
		Path:       "received.dat",
		ChunkIndex: 0,
		ChunkCount: 2,
		Data:       toB64(content[:65536]),
		Version:    1,
	}
	chunk1 := &FileChunkPayload{
		Path:       "received.dat",
		ChunkIndex: 1,
		ChunkCount: 2,
		Data:       toB64(content[65536:]),
		Version:    1,
	}

	tm.HandleFileChunk("sender", chunk0)
	tm.HandleFileChunk("sender", chunk1)

	// Verify the file was assembled correctly.
	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("read assembled file: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("assembled file content mismatch: want %d bytes, got %d bytes",
			len(content), len(got))
	}
	if hashOf(got) != hashOf(content) {
		t.Errorf("assembled file hash mismatch: want %s, got %s",
			hashOf(content), hashOf(got))
	}

	// Verify transfer was cleaned up.
	if tm.incoming.Get("received.dat", "sender", 1) != nil {
		t.Error("expected transfer to be removed after completion")
	}
}

func TestTransferManager_HandleUnknownChunk(t *testing.T) {
	tm := NewTransferManager("receiver", t.TempDir(), 65536,
		NewFileTracker(nil), nullBroadcast, nil, nil)

	// Sending a chunk without first sending an announcement should not panic.
	chunk := &FileChunkPayload{
		Path:       "unknown.dat",
		ChunkIndex: 0,
		ChunkCount: 1,
		Data:       toB64([]byte("hello")),
		Version:    99,
	}
	tm.HandleFileChunk("sender", chunk)
	// No crash = pass.
}

func TestTransferManager_FindMissingChunks(t *testing.T) {
	tm := NewTransferManager("receiver", t.TempDir(), 65536,
		NewFileTracker(nil), nullBroadcast, nil, nil)

	transfer := &IncomingTransfer{
		Path:       "test.dat",
		Version:    1,
		ChunkCount: 5,
		Received: map[int]bool{
			0: true,
			2: true,
			4: true,
		},
		NodeID: "sender",
	}

	missing := tm.findMissingChunks(transfer)
	expected := []int{1, 3}
	if len(missing) != len(expected) {
		t.Fatalf("want %d missing, got %d: %v", len(expected), len(missing), missing)
	}
	for i, idx := range expected {
		if missing[i] != idx {
			t.Errorf("missing[%d]: want %d, got %d", i, idx, missing[i])
		}
	}
}

func TestTransferManager_HandleFileChunk_OutOfOrder(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")
	content := testContent(200000) // 3 chunks (65536*3 = 196608, leftover 3392)

	expectedPath := filepath.Join(watchDir, "outoforder.dat")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	announce := &FileChangeAnnounce{
		Path:       "outoforder.dat",
		Tag:        "default",
		Size:       int64(len(content)),
		Hash:       hashOf(content),
		Version:    2,
		NodeID:     "sender",
		ChunkSize:  65536,
		ChunkCount: 3,
		ModTime:    time.Now().UnixNano(),
	}
	tm.HandleFileChange("sender", announce)

	// Send chunks out of order: 2, 0, 1
	chunks := []*FileChunkPayload{
		{Path: "outoforder.dat", ChunkIndex: 2, ChunkCount: 3, Data: toB64(content[131072:]), Version: 2},
		{Path: "outoforder.dat", ChunkIndex: 0, ChunkCount: 3, Data: toB64(content[:65536]), Version: 2},
		{Path: "outoforder.dat", ChunkIndex: 1, ChunkCount: 3, Data: toB64(content[65536:131072]), Version: 2},
	}

	for _, c := range chunks {
		tm.HandleFileChunk("sender", c)
	}

	// Verify assembled file.
	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("read assembled file: %v", err)
	}
	if hashOf(got) != hashOf(content) {
		t.Errorf("out-of-order assembly hash mismatch")
	}
}

func TestTransferManager_CleanStaleTransfers(t *testing.T) {
	tm := NewTransferManager("receiver", t.TempDir(), 65536,
		NewFileTracker(nil), nullBroadcast, nil, nil)

	// Manually add a transfer with an old timestamp.
	oldTransfer := &IncomingTransfer{
		Path:         "stale.dat",
		Version:      1,
		NodeID:       "sender",
		StartedAt:    time.Now().Add(-2 * time.Hour),
		LastActivity: time.Now().Add(-2 * time.Hour),
		Received:     make(map[int]bool),
	}
	tm.incoming.Set(oldTransfer)

	count := tm.CleanStaleTransfers(30 * time.Minute)
	if count != 1 {
		t.Errorf("want 1 stale transfer cleaned, got %d", count)
	}

	// Fresh transfer should not be cleaned.
	fresh := &IncomingTransfer{
		Path:         "fresh.dat",
		Version:      2,
		NodeID:       "sender",
		StartedAt:    time.Now(),
		LastActivity: time.Now(),
		Received:     make(map[int]bool),
	}
	tm.incoming.Set(fresh)

	count = tm.CleanStaleTransfers(30 * time.Minute)
	if count != 0 {
		t.Errorf("want 0 fresh transfers cleaned, got %d", count)
	}
}

func TestTransferManager_BroadcastOnPoll(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	// Create a file to track.
	content := []byte("hello, world")
	writeTestFile(t, watchDir, "greeting.txt", content)

	tracker := NewFileTracker([]WatchDir{
		{Path: watchDir, Tag: "default"},
	})

	var broadcastCount int
	var lastBroadcast []byte
	broadcast := func(data []byte) {
		broadcastCount++
		lastBroadcast = data
	}

	tm := NewTransferManager("sender", dataDir, 64,
		tracker, broadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	if err := tm.Poll(); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	if broadcastCount == 0 {
		t.Fatal("expected at least 1 broadcast (file_change + chunks)")
	}

	// First broadcast should be a file_change message.
	if lastBroadcast == nil {
		t.Fatal("no broadcast captured")
	}

	// Second poll should produce no changes.
	broadcastCount = 0
	if err := tm.Poll(); err != nil {
		t.Fatalf("second Poll failed: %v", err)
	}
	if broadcastCount != 0 {
		t.Errorf("expected 0 broadcasts on second poll, got %d", broadcastCount)
	}
}

func TestTransferManager_DuplicateAnnounce(t *testing.T) {
	tm := NewTransferManager("receiver", t.TempDir(), 65536,
		NewFileTracker(nil), nullBroadcast, nil, nil)

	announce := &FileChangeAnnounce{
		Path:    "dup.dat",
		Tag:     "default",
		Version: 1,
		NodeID:  "sender",
		Size:    100,
	}
	tm.HandleFileChange("sender", announce)
	tm.HandleFileChange("sender", announce)

	// Should only have one transfer entry.
	count := len(tm.incoming.List())
	if count != 1 {
		t.Errorf("expected 1 transfer, got %d", count)
	}
}

func TestTransferManager_HandleDeletion(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")

	// Create a file that should be deleted by the announcement.
	targetPath := filepath.Join(watchDir, "todelete.txt")
	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("delete me"), 0644); err != nil {
		t.Fatal(err)
	}

	tm := NewTransferManager("receiver", dir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Send a deletion announcement (Size == -1 signals deletion).
	announce := &FileChangeAnnounce{
		Path:    "todelete.txt",
		Tag:     "default",
		Size:    -1, // deletion signal
		Version: 3,
		NodeID:  "sender",
	}
	tm.HandleFileChange("sender", announce)

	// Verify file was deleted.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Errorf("expected file to be deleted, err=%v", err)
	}
}

func TestTransferManager_HandleDeletion_NonExistentFile(t *testing.T) {
	tm := NewTransferManager("receiver", t.TempDir(), 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: "/nonexistent/watch", Tag: "default"},
		}, nil)

	// Deleting a file that doesn't exist should not error.
	announce := &FileChangeAnnounce{
		Path:    "ghost.txt",
		Tag:     "default",
		Size:    -1,
		Version: 4,
		NodeID:  "sender",
	}
	tm.HandleFileChange("sender", announce)
	// No panic = pass.
}

// --- Conflict handling tests ------------------------------------------------

func TestTransferManager_Conflict_RenamesExisting(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	// Create an existing file at the destination with different content.
	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatal(err)
	}
	existingContent := []byte("this is the original content")
	destPath := filepath.Join(watchDir, "conflict.dat")
	if err := os.WriteFile(destPath, existingContent, 0644); err != nil {
		t.Fatal(err)
	}
	existingHash := hashOf(existingContent)

	// Incoming content is different.
	incomingContent := []byte("this is the incoming content from another node")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Announce the incoming file.
	announce := &FileChangeAnnounce{
		Path:       "conflict.dat",
		Tag:        "default",
		Size:       int64(len(incomingContent)),
		Hash:       hashOf(incomingContent),
		Version:    10,
		NodeID:     "node-a",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
	}
	tm.HandleFileChange("node-a", announce)

	// Send the single chunk.
	chunk := &FileChunkPayload{
		Path:       "conflict.dat",
		ChunkIndex: 0,
		ChunkCount: 1,
		Data:       toB64(incomingContent),
		Version:    10,
	}
	tm.HandleFileChunk("node-a", chunk)

	// Verify the incoming file was written to the destination.
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read destination file: %v", err)
	}
	if string(got) != string(incomingContent) {
		t.Errorf("destination content mismatch: want %q, got %q", incomingContent, got)
	}

	// Verify the original file was renamed to a conflict path.
	// Should match pattern: conflict.dat.conflict.node-a.<timestamp>
	conflictPattern := "conflict.dat.conflict.node-a."
	foundConflict := false
	entries, err := os.ReadDir(watchDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), conflictPattern) {
			foundConflict = true
			// Verify the conflict file has the original content.
			conflictData, err := os.ReadFile(filepath.Join(watchDir, e.Name()))
			if err != nil {
				t.Fatalf("read conflict file: %v", err)
			}
			if string(conflictData) != string(existingContent) {
				t.Errorf("conflict file content mismatch: want %q, got %q", existingContent, conflictData)
			}
			break
		}
	}
	if !foundConflict {
		t.Errorf("expected a conflict file with prefix %q in %s, got %v",
			conflictPattern, watchDir, listDirEntries(t, watchDir))
	}

	// Verify conflict was recorded.
	conflicts := tm.Conflicts()
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict record, got %d", len(conflicts))
	}
	if conflicts[0].Path != "conflict.dat" {
		t.Errorf("conflict Path: want conflict.dat, got %s", conflicts[0].Path)
	}
	if conflicts[0].IncomingNode != "node-a" {
		t.Errorf("conflict IncomingNode: want node-a, got %s", conflicts[0].IncomingNode)
	}
	if conflicts[0].LocalHash != existingHash {
		t.Errorf("conflict LocalHash: want %s, got %s", existingHash, conflicts[0].LocalHash)
	}
	if conflicts[0].IncomingHash != hashOf(incomingContent) {
		t.Errorf("conflict IncomingHash: want %s, got %s", hashOf(incomingContent), conflicts[0].IncomingHash)
	}
}

func TestTransferManager_Conflict_SameContentSkipsWrite(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	// Create an existing file at the destination with the same content as incoming.
	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatal(err)
	}
	content := []byte("this content is identical on both sides")
	destPath := filepath.Join(watchDir, "same.dat")
	origModTime := time.Now().Add(-1 * time.Hour)
	if err := os.WriteFile(destPath, content, 0644); err != nil {
		t.Fatal(err)
	}
	// Set a specific mtime so we can verify the file wasn't touched.
	if err := os.Chtimes(destPath, origModTime, origModTime); err != nil {
		t.Fatal(err)
	}

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	announce := &FileChangeAnnounce{
		Path:       "same.dat",
		Tag:        "default",
		Size:       int64(len(content)),
		Hash:       hashOf(content),
		Version:    10,
		NodeID:     "node-b",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
	}
	tm.HandleFileChange("node-b", announce)

	chunk := &FileChunkPayload{
		Path:       "same.dat",
		ChunkIndex: 0,
		ChunkCount: 1,
		Data:       toB64(content),
		Version:    10,
	}
	tm.HandleFileChunk("node-b", chunk)

	// Verify the destination file was NOT renamed (no conflict file).
	entries, err := os.ReadDir(watchDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".conflict.") {
			t.Errorf("unexpected conflict file created when content is identical: %s", e.Name())
		}
	}

	// Verify original file still exists with its original mtime.
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(origModTime) {
		t.Errorf("expected mtime %v, got %v — file was touched unnecessarily", origModTime, info.ModTime())
	}

	// Verify no conflict was recorded.
	if len(tm.Conflicts()) != 0 {
		t.Errorf("expected 0 conflicts for identical content, got %d", len(tm.Conflicts()))
	}
}

func TestTransferManager_Conflict_NoConflictWhenDestMissing(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	content := []byte("fresh file, no existing copy")
	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	announce := &FileChangeAnnounce{
		Path:       "new.dat",
		Tag:        "default",
		Size:       int64(len(content)),
		Hash:       hashOf(content),
		Version:    1,
		NodeID:     "node-c",
		ChunkSize:  65536,
		ChunkCount: 1,
		ModTime:    time.Now().UnixNano(),
	}
	tm.HandleFileChange("node-c", announce)

	chunk := &FileChunkPayload{
		Path:       "new.dat",
		ChunkIndex: 0,
		ChunkCount: 1,
		Data:       toB64(content),
		Version:    1,
	}
	tm.HandleFileChunk("node-c", chunk)

	// Verify file was written correctly.
	destPath := filepath.Join(watchDir, "new.dat")
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("content mismatch")
	}

	// Verify no conflict was recorded.
	if len(tm.Conflicts()) != 0 {
		t.Errorf("expected 0 conflicts for fresh file, got %d", len(tm.Conflicts()))
	}
}

func TestTransferManager_MultipleConflicts(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatal(err)
	}

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Set up: create 3 existing files with different content.
	files := []struct {
		name    string
		existing []byte
		incoming []byte
		nodeID  string
	}{
		{"file1.txt", []byte("original content A"), []byte("incoming content A"), "node-a"},
		{"file2.txt", []byte("original content B"), []byte("incoming content B"), "node-b"},
		{"file3.txt", []byte("original content C"), []byte("incoming content C"), "node-c"},
	}

	for _, f := range files {
		destPath := filepath.Join(watchDir, f.name)
		if err := os.WriteFile(destPath, f.existing, 0644); err != nil {
			t.Fatal(err)
		}

		announce := &FileChangeAnnounce{
			Path:       f.name,
			Tag:        "default",
			Size:       int64(len(f.incoming)),
			Hash:       hashOf(f.incoming),
			Version:    5,
			NodeID:     f.nodeID,
			ChunkSize:  65536,
			ChunkCount: 1,
			ModTime:    time.Now().UnixNano(),
		}
		tm.HandleFileChange(f.nodeID, announce)

		chunk := &FileChunkPayload{
			Path:       f.name,
			ChunkIndex: 0,
			ChunkCount: 1,
			Data:       toB64(f.incoming),
			Version:    5,
		}
		tm.HandleFileChunk(f.nodeID, chunk)
	}

	// Verify all 3 conflicts recorded.
	conflicts := tm.Conflicts()
	if len(conflicts) != 3 {
		t.Fatalf("expected 3 conflicts, got %d", len(conflicts))
	}

	// Verify incoming files were written correctly.
	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(watchDir, f.name))
		if err != nil {
			t.Errorf("read %s: %v", f.name, err)
			continue
		}
		if string(got) != string(f.incoming) {
			t.Errorf("%s: expected incoming content, got %q", f.name, got)
		}
	}

	// Verify conflict files exist.
	entries, err := os.ReadDir(watchDir)
	if err != nil {
		t.Fatal(err)
	}
	conflictCount := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), ".conflict.") {
			conflictCount++
		}
	}
	if conflictCount != 3 {
		t.Errorf("expected 3 conflict files on disk, got %d: %v", conflictCount, listDirEntries(t, watchDir))
	}
}

// listDirEntries returns a comma-separated list of directory entry names (test helper).
func listDirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
