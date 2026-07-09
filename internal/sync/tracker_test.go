package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// stubHash returns a fixed hash so tests don't depend on file content.
func stubHash(checksum string) HashFunc {
	return func(io.Reader) (string, error) {
		return checksum, nil
	}
}

// writeFile creates a file at path with the given content, creating dirs.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

// countByType groups events by their type.
func countByType(events []ChangeEvent) map[ChangeType]int {
	m := map[ChangeType]int{ChangeCreated: 0, ChangeModified: 0, ChangeDeleted: 0}
	for _, e := range events {
		m[e.Type]++
	}
	return m
}

// eventPaths returns the paths in an event slice, sorted.
func eventPaths(events []ChangeEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Path
	}
	sort.Strings(out)
	return out
}

// --- Tests -----------------------------------------------------------------

func TestNewFileTracker_DeduplicatesDirs(t *testing.T) {
	ft := NewFileTracker([]WatchDir{
		{Path: "/a", Tag: "first"},
		{Path: "/a", Tag: "second"},
		{Path: "/b", Tag: "B"},
	})
	dirs := ft.Dirs()
	if len(dirs) != 2 {
		t.Fatalf("want 2 dirs, got %d", len(dirs))
	}
	// "first" tag should win for /a.
	for _, d := range dirs {
		if d.Path == "/a" && d.Tag != "first" {
			t.Errorf("want tag 'first' for /a, got %q", d.Tag)
		}
	}
}

func TestScan_FirstScanReturnsCreated(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "a.txt"), "hello")
	writeFile(t, filepath.Join(dir, "b.txt"), "world")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "docs"}})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	counts := countByType(events)
	if counts[ChangeCreated] != 2 {
		t.Errorf("want 2 created, got %d", counts[ChangeCreated])
	}
	if counts[ChangeModified] != 0 {
		t.Errorf("want 0 modified, got %d", counts[ChangeModified])
	}
	if counts[ChangeDeleted] != 0 {
		t.Errorf("want 0 deleted, got %d", counts[ChangeDeleted])
	}

	// Each event should have a File with Version > 0.
	for _, e := range events {
		if e.File == nil {
			t.Errorf("created event for %s missing File", e.Path)
			continue
		}
		if e.File.Version == 0 {
			t.Errorf("file %s has version 0", e.Path)
		}
		if e.File.Tag != "docs" {
			t.Errorf("file %s has tag %q, want docs", e.Path, e.File.Tag)
		}
	}
}

func TestScan_SecondScanNoChanges(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.txt"), "hello")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "data"}})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("first scan: want 1 event, got %d", len(events))
	}

	// Second scan — no changes.
	events, err = ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Fatalf("second scan: want nil (no changes), got %d events", len(events))
	}
}

func TestScan_DetectsModifiedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")
	writeFile(t, path, "v1")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "data"}})
	ft.Scan()

	// Wait a tick so mtime changes (some filesystems have 1s granularity).
	time.Sleep(time.Second)

	writeFile(t, path, "v2")

	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].Type != ChangeModified {
		t.Errorf("want modified, got %v", events[0].Type)
	}
	if events[0].File == nil {
		t.Fatal("event missing File")
	}
	if events[0].File.Version < 2 {
		t.Errorf("want version >= 2, got %d", events[0].File.Version)
	}
}

func TestScan_DetectsDeletedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")
	writeFile(t, path, "hello")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "data"}})
	ft.Scan()

	// Delete the file.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].Type != ChangeDeleted {
		t.Errorf("want deleted, got %v", events[0].Type)
	}

	// Confirm Snapshot shows it as deleted.
	snap := ft.Snapshot()
	found := false
	for _, tf := range snap {
		if tf.Path == path {
			found = true
			if !tf.Deleted {
				t.Error("expected Deleted=true in snapshot")
			}
			break
		}
	}
	if !found {
		t.Error("deleted file not found in Snapshot")
	}
}

func TestScan_DeletedThenRecreated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")
	writeFile(t, path, "hello")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "data"}})
	ft.Scan()

	os.Remove(path)

	// Scan marks it deleted.
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != ChangeDeleted {
		t.Fatalf("want 1 deleted, got %d events", len(events))
	}

	// Recreate.
	writeFile(t, path, "hello again")

	events, err = ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event after recreate, got %d", len(events))
	}
	if events[0].Type != ChangeCreated {
		t.Errorf("want created after recreate, got %v", events[0].Type)
	}
}

func TestScan_DetectsMultipleChanges(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "keep.txt"), "same")
	writeFile(t, filepath.Join(dir, "mod.txt"), "old")
	writeFile(t, filepath.Join(dir, "del.txt"), "bye")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "test"}})
	ft.Scan()

	time.Sleep(time.Second)

	// Modify one, create one, delete one.
	writeFile(t, filepath.Join(dir, "mod.txt"), "new")
	writeFile(t, filepath.Join(dir, "new.txt"), "new file")
	os.Remove(filepath.Join(dir, "del.txt"))

	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	counts := countByType(events)
	if counts[ChangeCreated] != 1 {
		t.Errorf("want 1 created, got %d", counts[ChangeCreated])
	}
	if counts[ChangeModified] != 1 {
		t.Errorf("want 1 modified, got %d", counts[ChangeModified])
	}
	if counts[ChangeDeleted] != 1 {
		t.Errorf("want 1 deleted, got %d", counts[ChangeDeleted])
	}
}

func TestScan_NonExistentDir(t *testing.T) {
	ft := NewFileTracker([]WatchDir{{Path: "/nonexistent/xyz", Tag: "ghost"}})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Fatalf("want nil for nonexistent dir, got %d events", len(events))
	}
}

func TestSnapshot_ReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "f.txt"), "data")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "x"}})
	ft.Scan()

	snap := ft.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 file in snapshot, got %d", len(snap))
	}

	// Mutating the returned slice should not affect the tracker.
	snap[0].Tag = "hacked"
	if ft.Get(snap[0].Path).Tag == "hacked" {
		t.Error("snapshot mutation leaked into tracker")
	}
}

func TestGet_UnknownPath(t *testing.T) {
	ft := NewFileTracker([]WatchDir{{Path: t.TempDir(), Tag: "x"}})
	ft.Scan()
	if tf := ft.Get("/never/scanned"); tf != nil {
		t.Errorf("expected nil, got %+v", tf)
	}
}

func TestReset_ClearsAllState(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "f.txt"), "data")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "x"}})
	ft.Scan()

	snap := ft.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 file after scan, got %d", len(snap))
	}

	ft.Reset()

	snap = ft.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("want 0 after reset, got %d", len(snap))
	}

	// Next scan should report all files as created.
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != ChangeCreated {
		t.Fatalf("want 1 created after reset, got %d events", len(events))
	}
}

func TestScan_HashChangesOnContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "data"}})

	// Write v1, scan.
	writeFile(t, path, "version 1")
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != ChangeCreated {
		t.Fatalf("first scan: want 1 created, got %d", len(events))
	}
	v1Hash := events[0].File.Hash

	// Same content, different mtime — should NOT trigger (we hash-content check).
	time.Sleep(time.Second)
	writeFile(t, path, "version 1") // same content

	events, err = ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Errorf("same content should not trigger change, got %d events", len(events))
	}

	// Different content.
	time.Sleep(time.Second)
	writeFile(t, path, "version 2")

	events, err = ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != ChangeModified {
		t.Fatalf("different content: want 1 modified, got %d", len(events))
	}
	if events[0].File.Hash == v1Hash {
		t.Error("hash should differ after content change")
	}
}

func TestScan_SkipsHiddenDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create a hidden dir with a file — should be skipped.
	hidden := filepath.Join(dir, ".hidden")
	writeFile(t, filepath.Join(hidden, "secret.txt"), "shh")

	// Visible file.
	writeFile(t, filepath.Join(dir, "visible.txt"), "hello")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "test"}})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event (visible file only), got %d", len(events))
	}
	paths := eventPaths(events)
	if filepath.Base(paths[0]) != "visible.txt" {
		t.Errorf("expected visible.txt, got %s", paths[0])
	}
}

func TestScan_SkipsNonRegularFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a symlink (will be skipped).
	writeFile(t, filepath.Join(dir, "real.txt"), "real")
	if err := os.Symlink(filepath.Join(dir, "real.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "test"}})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event (regular file only), got %d", len(events))
	}
}

func TestMultipleWatchDirs_UniquePaths(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()

	writeFile(t, filepath.Join(dirA, "a.txt"), "from A")
	writeFile(t, filepath.Join(dirB, "b.txt"), "from B")

	ft := NewFileTracker([]WatchDir{
		{Path: dirA, Tag: "dirA"},
		{Path: dirB, Tag: "dirB"},
	})
	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}

	snap := ft.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("want 2 files in snapshot, got %d", len(snap))
	}
}

func TestVersionCounter_Monotonic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")
	writeFile(t, path, "v1")

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "x"}})

	ev1, _ := ft.Scan()
	v1 := ev1[0].File.Version

	// Modify.
	time.Sleep(time.Second)
	writeFile(t, path, "v2")
	ev2, _ := ft.Scan()
	v2 := ev2[0].File.Version

	// Modify again.
	time.Sleep(time.Second)
	writeFile(t, path, "v3")
	ev3, _ := ft.Scan()
	v3 := ev3[0].File.Version

	if !(v1 < v2 && v2 < v3) {
		t.Errorf("versions not monotonic: %d < %d < %d = %v", v1, v2, v3, v1 < v2 && v2 < v3)
	}
}

// --- Real hash integration test -------------------------------------------

func TestHashFile_ProducesCorrectSHA256(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hashcheck.txt")
	content := "hello, flare\n"
	writeFile(t, path, content)

	// Compute expected hash.
	h := sha256.Sum256([]byte(content))
	expected := hex.EncodeToString(h[:])

	tf, err := hashFile(path, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	if tf.Hash != expected {
		t.Errorf("hash mismatch:\n  want: %s\n  got:  %s", expected, tf.Hash)
	}
	if tf.Size != int64(len(content)) {
		t.Errorf("size mismatch: want %d, got %d", len(content), tf.Size)
	}
	if tf.Path != path {
		t.Errorf("path mismatch")
	}
}

func TestHashFile_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	writeFile(t, path, "")

	tf, err := hashFile(path, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	// SHA-256 of empty string.
	expected := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if tf.Hash != expected {
		t.Errorf("empty file hash: want %s, got %s", expected, tf.Hash)
	}
	if tf.Size != 0 {
		t.Errorf("empty file size: want 0, got %d", tf.Size)
	}
}
