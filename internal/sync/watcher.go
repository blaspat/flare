// Package sync provides file-system tracking and synchronisation for the Flare
// edge mesh.
package sync

import (
	"log/slog"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// DirWatcher monitors watch directories for file-system changes using
// platform-specific notifications (inotify on Linux, kqueue on macOS/BSD)
// via fsnotify. When the OS mechanism fails (e.g. on NFS, FUSE, or WSL1),
// it transparently falls back to polling.
//
// The watcher does NOT report individual file events — it signals that
// "something changed" so the caller can run a full FileTracker.Scan() to
// detect the actual differences. This avoids duplicating the content-hash
// comparison logic that already lives in Scan().
//
// Events from rapid write sequences (e.g. editor saves that create temp
// files and rename) are debounced so only one trigger is emitted per
// burst.
type DirWatcher struct {
	mu      sync.Mutex
	dirs    []WatchDir
	trigger chan struct{} // buffered 1 — signals a scan is needed
	done    chan struct{} // closed on Stop

	interval time.Duration // poll fallback interval & debounce cooldown

	fsw   *fsnotify.Watcher // nil when using polling fallback
	start sync.Once
	stop  sync.Once
}

// NewDirWatcher creates a watcher that monitors the given directories.
//
//   - dirs: the directories to watch (the same WatchDir type used by FileTracker).
//   - fallbackInterval: used both as the polling period when fsnotify is
//     unavailable and as the debounce window for coalescing rapid events.
func NewDirWatcher(dirs []WatchDir, fallbackInterval time.Duration) *DirWatcher {
	if fallbackInterval <= 0 {
		fallbackInterval = 5 * time.Second
	}
	return &DirWatcher{
		dirs:     dirs,
		interval: fallbackInterval,
		trigger:  make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// Start begins watching. It first attempts to use fsnotify for each watch
// directory; if any directory fails (unsupported filesystem) or the OS
// mechanism is not available, the watcher falls back to polling.
//
// Start is safe to call multiple times — only the first call has effect.
func (dw *DirWatcher) Start() {
	dw.start.Do(func() {
		fsw, err := fsnotify.NewWatcher()
		if err != nil {
			slog.Warn("fsnotify not available, falling back to polling",
				"err", err)
			go dw.pollLoop()
			return
		}

		// Try to add all watch directories. If any fail, fall back to
		// polling for the entire watcher (simpler than a per-dir split).
		var allOK bool
		for _, d := range dw.dirs {
			if addErr := fsw.Add(d.Path); addErr != nil {
				slog.Warn("fsnotify cannot watch directory, falling back to polling",
					"dir", d.Path, "err", addErr)
				fsw.Close()
				allOK = false
				break
			}
			allOK = true
		}

		if !allOK {
			go dw.pollLoop()
			return
		}

		dw.mu.Lock()
		dw.fsw = fsw
		dw.mu.Unlock()
		slog.Info("file watcher started with fsnotify",
			"dirs", len(dw.dirs), "debounce", dw.interval)
		go dw.fsnotifyLoop(fsw)
	})
}

// C returns a channel that receives a signal (an empty struct) whenever one
// of the watched directories has changed. The caller should respond by
// calling FileTracker.Scan() (or TransferManager.Poll()).
//
// Multiple file events within the debounce window are coalesced into a
// single trigger.
func (dw *DirWatcher) C() <-chan struct{} {
	return dw.trigger
}

// Stop terminates the watcher and releases all OS resources (inotify
// descriptors, kqueue fds, etc.). Safe to call multiple times.
func (dw *DirWatcher) Stop() {
	dw.stop.Do(func() {
		close(dw.done)
		dw.mu.Lock()
		if dw.fsw != nil {
			dw.fsw.Close()
			dw.fsw = nil
		}
		dw.mu.Unlock()
	})
}

// fsnotifyLoop reads events from the fsnotify watcher, debounces them, and
// sends a single trigger on the trigger channel after activity settles.
func (dw *DirWatcher) fsnotifyLoop(fsw *fsnotify.Watcher) {
	defer fsw.Close()

	var debounce *time.Timer
	var debounceC <-chan time.Time

	flush := func() {
		if debounce != nil {
			debounce.Stop()
			debounce = nil
			debounceC = nil
		}
		// Non-blocking send: if there's already a pending trigger, drop it.
		select {
		case dw.trigger <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case event, ok := <-fsw.Events:
			if !ok {
				return
			}
			// Ignore CHMOD events — they don't reflect content changes.
			if event.Op&fsnotify.Chmod != 0 && event.Op == fsnotify.Chmod {
				continue
			}
			slog.Debug("watcher event", "op", event.Op, "path", event.Name)

			// Reset debounce timer on each event.
			if debounce == nil {
				debounce = time.NewTimer(dw.interval)
				debounceC = debounce.C
			} else {
				debounce.Reset(dw.interval)
			}

		case err, ok := <-fsw.Errors:
			if !ok {
				return
			}
			slog.Warn("fsnotify error", "err", err)
			// On error, fire a trigger anyway — better to re-scan than
			// to miss a change.
			flush()

		case <-debounceC:
			flush()

		case <-dw.done:
			return
		}
	}
}

// pollLoop periodically fires the trigger channel as a fallback when
// fsnotify is not available.
func (dw *DirWatcher) pollLoop() {
	ticker := time.NewTicker(dw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case dw.trigger <- struct{}{}:
			default:
			}
		case <-dw.done:
			return
		}
	}
}

// WatchStatus describes which mechanism the watcher is using.
type WatchStatus int

const (
	WatchUnknown    WatchStatus = iota
	WatchFsnotify               // inotify / kqueue
	WatchPolling                // timer-based fallback
)

// Status returns the current watch mechanism. Useful for logging / CLI
// status output.
func (dw *DirWatcher) Status() WatchStatus {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	if dw.fsw != nil {
		return WatchFsnotify
	}
	return WatchPolling
}

func (s WatchStatus) String() string {
	switch s {
	case WatchFsnotify:
		return "fsnotify"
	case WatchPolling:
		return "polling"
	default:
		return "unknown"
	}
}
