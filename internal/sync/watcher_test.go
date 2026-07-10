package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewDirWatcher_Defaults(t *testing.T) {
	dw := NewDirWatcher(nil, 0)
	if dw.interval != 5*time.Second {
		t.Errorf("default interval: want 5s, got %v", dw.interval)
	}
	if cap(dw.trigger) != 1 {
		t.Errorf("trigger cap: want 1, got %d", cap(dw.trigger))
	}
}

func TestDirWatcher_StartStop_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 100*time.Millisecond)

	// Start twice (should be idempotent).
	dw.Start()
	dw.Start()

	// Should be using fsnotify on any modern OS.
	status := dw.Status()
	if status != WatchFsnotify && status != WatchPolling {
		t.Errorf("unexpected status: %v", status)
	}

	// Stop twice (should be idempotent).
	dw.Stop()
	dw.Stop()
}

func TestDirWatcher_TriggersOnFileCreate(t *testing.T) {
	dir := t.TempDir()
	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// Create a file — should trigger after debounce.
	testFile := filepath.Join(dir, "new.txt")
	if err := os.WriteFile(testFile, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	select {
	case <-dw.C():
		// Success — trigger fired.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trigger on file create")
	}
}

func TestDirWatcher_TriggersOnFileModify(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "mod.txt")
	if err := os.WriteFile(testFile, []byte("v1"), 0644); err != nil {
		t.Fatal(err)
	}

	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// Drain any initial trigger from watching the existing file.
	select {
	case <-dw.C():
	default:
	}

	// Modify the file.
	time.Sleep(50 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("v2"), 0644); err != nil {
		t.Fatal(err)
	}

	select {
	case <-dw.C():
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trigger on file modify")
	}
}

func TestDirWatcher_TriggersOnFileDelete(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "del.txt")
	if err := os.WriteFile(testFile, []byte("bye"), 0644); err != nil {
		t.Fatal(err)
	}

	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// Drain initial trigger.
	select {
	case <-dw.C():
	default:
	}

	// Delete the file.
	if err := os.Remove(testFile); err != nil {
		t.Fatal(err)
	}

	select {
	case <-dw.C():
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trigger on file delete")
	}
}

func TestDirWatcher_DebounceCoalescesEvents(t *testing.T) {
	dir := t.TempDir()
	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 200*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// Drain initial trigger.
	select {
	case <-dw.C():
	default:
	}

	// Write several files rapidly (simulates a multi-file save operation).
	for i := 0; i < 10; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%d.txt", i))
		_ = os.WriteFile(p, []byte("data"), 0644)
	}

	// Wait for the debounce window to elapse.
	time.Sleep(500 * time.Millisecond)

	// Should receive exactly 1 trigger (debounced), not 10.
	select {
	case <-dw.C():
		// Got one trigger.
	default:
		t.Fatal("expected a trigger after debounce window")
	}

	// Make sure there isn't a second trigger immediately.
	select {
	case <-dw.C():
		t.Fatal("debounce failed: got multiple triggers for one burst")
	case <-time.After(300 * time.Millisecond):
		// Good — single trigger only.
	}
}

func TestDirWatcher_NoTriggerForChmodOnly(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "perm.txt")
	if err := os.WriteFile(testFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// Drain initial trigger.
	select {
	case <-dw.C():
	default:
	}

	// CHMOD should NOT trigger a scan.
	if err := os.Chmod(testFile, 0600); err != nil {
		t.Fatal(err)
	}

	// Wait long enough for debounce to have fired if it were going to.
	time.Sleep(300 * time.Millisecond)

	select {
	case <-dw.C():
		t.Fatal("CHMOD-only event should not trigger a scan")
	default:
		// Good — no trigger for permission-only change.
	}
}

func TestDirWatcher_NonExistentDir_FallsBackToPolling(t *testing.T) {
	// A non-existent directory should cause fsnotify to fail and fall
	// back to polling.
	dir := filepath.Join(t.TempDir(), "nonexistent")
	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "ghost"}}, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	if dw.Status() != WatchPolling {
		t.Logf("watcher status: %v (may differ by platform)", dw.Status())
	}

	// Polling fallback should still fire triggers eventually.
	select {
	case <-dw.C():
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal("polling fallback did not fire trigger")
	}
}

func TestDirWatcher_StopPreventsFurtherTriggers(t *testing.T) {
	dir := t.TempDir()
	dw := NewDirWatcher([]WatchDir{{Path: dir, Tag: "test"}}, 50*time.Millisecond)
	dw.Start()
	dw.Stop()

	// Create a file after stop — should NOT trigger.
	testFile := filepath.Join(dir, "afterstop.txt")
	if err := os.WriteFile(testFile, []byte("oops"), 0644); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)

	select {
	case <-dw.C():
		t.Fatal("trigger fired after watcher was stopped")
	default:
		// Good — no trigger after stop.
	}
}

func TestDirWatcher_EmptyDirsList_StillRuns(t *testing.T) {
	dw := NewDirWatcher(nil, 50*time.Millisecond)
	dw.Start()
	defer dw.Stop()

	// No crash is the test.
	status := dw.Status()
	if status != WatchPolling && status != WatchUnknown {
		t.Logf("watcher status with empty dirs: %v", status)
	}
}
