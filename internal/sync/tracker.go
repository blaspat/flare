// Package sync provides file-system tracking and synchronisation for the Flare
// edge mesh. It watches directories, detects changes via content hashing, and
// produces change events that drive the distributed file-sync protocol.
package sync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// --- Change types ----------------------------------------------------------

// ChangeType describes what happened to a file between two scans.
type ChangeType int

const (
	ChangeCreated  ChangeType = iota + 1 // file appeared
	ChangeModified                        // content or metadata changed
	ChangeDeleted                         // file disappeared
)

var changeTypeNames = map[ChangeType]string{
	ChangeCreated:  "created",
	ChangeModified: "modified",
	ChangeDeleted:  "deleted",
}

func (ct ChangeType) String() string {
	if s, ok := changeTypeNames[ct]; ok {
		return s
	}
	return fmt.Sprintf("ChangeType(%d)", int(ct))
}

// --- TrackedFile -----------------------------------------------------------

// TrackedFile holds the metadata snapshot for a single tracked file.
type TrackedFile struct {
	Path    string    `json:"path"`    // absolute path
	Tag     string    `json:"tag"`     // watch-dir tag from config
	Size    int64     `json:"size"`    // file size in bytes
	ModTime time.Time `json:"mod_time"` // modification time (wall clock)
	Hash    string    `json:"hash"`    // hex-encoded SHA-256
	Version uint64    `json:"version"` // monotonic version for causal ordering
	Deleted bool      `json:"deleted"` // true when the file no longer exists
}

// --- ChangeEvent -----------------------------------------------------------

// ChangeEvent describes a single file change detected during a scan.
type ChangeEvent struct {
	Type ChangeType   `json:"type"`
	Path string       `json:"path"`
	Tag  string       `json:"tag"`
	File *TrackedFile `json:"file,omitempty"` // set for created/modified; nil for deleted
}

// --- WatchDir --------------------------------------------------------------

// WatchDir is a directory to watch, paired with an opaque tag.
type WatchDir struct {
	Path string
	Tag  string
}

// --- FileTracker -----------------------------------------------------------

// FileTracker watches directories and maintains a cached view of tracked files.
// Each call to Scan walks every watch directory, computes SHA-256 hashes,
// compares against the previous snapshot, and returns the differences.
//
// The first scan returns every file as ChangeCreated.
type FileTracker struct {
	mu          sync.RWMutex
	dirs        []WatchDir
	files       map[string]*TrackedFile      // absolute path → tracked file
	version     uint64                       // global version counter
	hashFunc    func(io.Reader) (string, error)
	ignoreRules map[string]*IgnoreRules      // watch-dir path → rules
	cryptoMgr   *CryptoManager               // nil = encryption disabled
}

// HashFunc is the signature for hash functions passed to the option.
type HashFunc func(io.Reader) (string, error)

// Option configures a FileTracker.
type Option func(*FileTracker)

// WithHashFunc overrides the default SHA-256 hash function (used in tests).
func WithHashFunc(fn HashFunc) Option {
	return func(ft *FileTracker) {
		ft.hashFunc = fn
	}
}

// WithCryptoManager enables transparent decryption when reading files for
// hashing. Pass a CryptoManager created with the configured encryption key,
// or nil to leave encryption disabled.
func WithCryptoManager(cm *CryptoManager) Option {
	return func(ft *FileTracker) {
		ft.cryptoMgr = cm
	}
}

// NewFileTracker creates a tracker for the given directories.
// Duplicate watch-dir paths are silently deduplicated (first tag wins).
func NewFileTracker(dirs []WatchDir, opts ...Option) *FileTracker {
	// Deduplicate by path.
	seen := make(map[string]WatchDir, len(dirs))
	for _, d := range dirs {
		if _, ok := seen[d.Path]; !ok {
			seen[d.Path] = d
		}
	}
	resolved := make([]WatchDir, 0, len(seen))
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	sort.Strings(paths) // deterministic ordering
	for _, p := range paths {
		resolved = append(resolved, seen[p])
	}

	ft := &FileTracker{
		dirs:        resolved,
		files:       make(map[string]*TrackedFile),
		hashFunc:    sha256Hash,
		ignoreRules: make(map[string]*IgnoreRules, len(resolved)),
	}
	for _, o := range opts {
		o(ft)
	}

	// Load .flareignore rules for each watch directory.
	for _, d := range resolved {
		ft.ignoreRules[d.Path] = LoadIgnoreForDir(d.Path)
	}

	return ft
}

// Dirs returns a copy of the watch directories.
func (ft *FileTracker) Dirs() []WatchDir {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	out := make([]WatchDir, len(ft.dirs))
	copy(out, ft.dirs)
	return out
}

// Scan walks all watched directories, computes hashes, and returns the
// changes since the last scan. The first call marks every file as created.
//
// Scan is safe for concurrent use, but concurrent calls produce undefined
// results — the caller should serialise scans.
func (ft *FileTracker) Scan() ([]ChangeEvent, error) {
	// Build a snapshot of what exists on disk.
	disk, err := ft.walkAll()
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	var events []ChangeEvent

	// 1. Detect created and modified files.
	for absPath, tf := range disk {
		existing, known := ft.files[absPath]
		if !known {
			// New file — created.
			tf.Version = ft.nextVersion()
			ft.files[absPath] = tf
			events = append(events, ChangeEvent{
				Type: ChangeCreated,
				Path: absPath,
				Tag:  tf.Tag,
				File: cloneTF(tf),
			})
			continue
		}

		// Check if the file actually changed.
		// We compare content hash + size, not mtime — hash covers content.
		changed := existing.Hash != tf.Hash || existing.Size != tf.Size

		if existing.Deleted {
			// File was deleted and has reappeared — treat as created.
			tf.Version = ft.nextVersion()
			ft.files[absPath] = tf
			events = append(events, ChangeEvent{
				Type: ChangeCreated,
				Path: absPath,
				Tag:  tf.Tag,
				File: cloneTF(tf),
			})
		} else if changed {
			tf.Version = ft.nextVersion()
			ft.files[absPath] = tf
			events = append(events, ChangeEvent{
				Type: ChangeModified,
				Path: absPath,
				Tag:  tf.Tag,
				File: cloneTF(tf),
			})
		}
	}

	// 2. Detect deleted files (known but gone from disk).
	for absPath, existing := range ft.files {
		if existing.Deleted {
			continue // already marked
		}
		if _, onDisk := disk[absPath]; !onDisk {
			existing.Deleted = true
			existing.Version = ft.nextVersion()
			events = append(events, ChangeEvent{
				Type: ChangeDeleted,
				Path: absPath,
				Tag:  existing.Tag,
			})
		}
	}

	if len(events) == 0 {
		return nil, nil
	}
	return events, nil
}

// Snapshot returns a copy of all tracked files (including deleted ones).
func (ft *FileTracker) Snapshot() []TrackedFile {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	out := make([]TrackedFile, 0, len(ft.files))
	paths := make([]string, 0, len(ft.files))
	for p := range ft.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		out = append(out, *ft.files[p])
	}
	return out
}

// Get returns the tracked file for an absolute path, or nil if unknown.
func (ft *FileTracker) Get(absPath string) *TrackedFile {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	tf, ok := ft.files[absPath]
	if !ok {
		return nil
	}
	c := *tf
	return &c
}

// GetByTagAndPath looks up a tracked file by its tag and relative path.
// Returns nil if not found.
func (ft *FileTracker) GetByTagAndPath(tag, relPath string) *TrackedFile {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	// Find the base directory for this tag.
	var baseDir string
	for _, d := range ft.dirs {
		if d.Tag == tag {
			baseDir = d.Path
			break
		}
	}
	if baseDir == "" {
		return nil
	}
	absPath := filepath.Join(baseDir, relPath)
	tf, ok := ft.files[absPath]
	if !ok {
		return nil
	}
	c := *tf
	return &c
}

// Reset clears all tracked state. The next scan will report every file as
// created.
func (ft *FileTracker) Reset() {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.files = make(map[string]*TrackedFile)
	ft.version = 0
}

// Save persists the current tracker state to a JSON file.
// The state includes all tracked files (including tombstones) so that
// offline deletions can be detected on restart.
func (ft *FileTracker) Save(path string) error {
	ft.mu.RLock()
	snap := make(map[string]TrackedFile)
	for k, v := range ft.files {
		snap[k] = *v
	}
	ver := ft.version
	ft.mu.RUnlock()

	data, err := json.Marshal(struct {
		Version uint64                  `json:"version"`
		Files   map[string]TrackedFile `json:"files"`
	}{Version: ver, Files: snap})
	if err != nil {
		return fmt.Errorf("marshal tracker state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write tracker state: %w", err)
	}
	return nil
}

// Load restores tracker state from a JSON file previously written by Save.
// It does NOT walk the watch directories — call Scan() afterwards to detect
// offline changes.
func (ft *FileTracker) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first run — no state to load
		}
		return fmt.Errorf("read tracker state: %w", err)
	}
	var loaded struct {
		Version uint64                  `json:"version"`
		Files   map[string]TrackedFile `json:"files"`
	}
	if err := json.Unmarshal(data, &loaded); err != nil {
		return fmt.Errorf("unmarshal tracker state: %w", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.version = loaded.Version
	ft.files = make(map[string]*TrackedFile, len(loaded.Files))
	for k, v := range loaded.Files {
		v := v
		ft.files[k] = &v
	}
	return nil
}

// nextVersion bumps and returns the global version counter.
func (ft *FileTracker) nextVersion() uint64 {
	ft.version++
	return ft.version
}

// walkAll walks every watch directory and returns the current state on disk.
func (ft *FileTracker) walkAll() (map[string]*TrackedFile, error) {
	all := make(map[string]*TrackedFile)
	for _, dir := range ft.dirs {
		entries, err := walkDir(dir, ft)
		if err != nil {
			return nil, err
		}
		for absPath, tf := range entries {
			// Keep the first occurrence if paths overlap across watch dirs.
			if _, exists := all[absPath]; !exists {
				all[absPath] = tf
			}
		}
	}
	return all, nil
}

// walkDir walks a single directory and returns its files as tracked entries.
func walkDir(dir WatchDir, ft *FileTracker) (map[string]*TrackedFile, error) {
	info, err := os.Stat(dir.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*TrackedFile), nil // dir doesn't exist yet
		}
		return nil, fmt.Errorf("stat watch dir %q: %w", dir.Path, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("watch path is not a directory: %q", dir.Path)
	}

	entries := make(map[string]*TrackedFile)

	err = filepath.WalkDir(dir.Path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err // propagate
		}

		// Skip .flareignore meta-files.
		if !d.IsDir() && d.Name() == ".flareignore" {
			return nil
		}

		if d.IsDir() {
			// Skip hidden directories (starting with .).
			if d.Name()[0] == '.' && path != dir.Path {
				return fs.SkipDir
			}
			// Skip directories matched by ignore rules.
			rules := ft.ignoreRules[dir.Path]
			if rules != nil && rules.Match(path, true) {
				return fs.SkipDir
			}
			return nil
		}

		// Only regular files.
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		absPath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("abs %q: %w", path, err)
		}

		// Skip files matched by ignore rules.
		rules := ft.ignoreRules[dir.Path]
		if rules != nil && rules.Match(absPath, false) {
			return nil
		}

		tf, err := hashFile(absPath, info, dir.Tag, ft)
		if err != nil {
			return fmt.Errorf("hash %q: %w", absPath, err)
		}
		entries[absPath] = tf
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %q: %w", dir.Path, err)
	}

	return entries, nil
}

// hashFile reads the file, computes its hash, and returns a TrackedFile.
// The Version field is left at 0 — the caller sets it.
// If ft is non-nil, the configured hash function is used; otherwise SHA-256.
// When ft.cryptoMgr is enabled, the file is decrypted before hashing
// (files on disk are encrypted at rest).
func hashFile(absPath string, info os.FileInfo, tag string, ft *FileTracker) (*TrackedFile, error) {
	if info == nil {
		var err error
		info, err = os.Stat(absPath)
		if err != nil {
			return nil, fmt.Errorf("stat %q: %w", absPath, err)
		}
	}

	tf := &TrackedFile{
		Path:    absPath,
		Tag:     tag,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}

	if info.Size() == 0 {
		tf.Hash = sha256Hex(nil)
		return tf, nil
	}

	var hashFn HashFunc
	if ft != nil && ft.hashFunc != nil {
		hashFn = ft.hashFunc
	} else {
		hashFn = sha256Hash
	}

	// When encryption is enabled, read and decrypt first, then hash.
	if ft != nil && ft.cryptoMgr != nil && ft.cryptoMgr.Enabled() {
		plain, err := ft.cryptoMgr.ReadDecryptedWithFallback(absPath)
		if err != nil {
			return nil, fmt.Errorf("read/decrypt %q: %w", absPath, err)
		}
		tf.Size = int64(len(plain))
		h, err := hashFn(bytes.NewReader(plain))
		if err != nil {
			return nil, fmt.Errorf("hash %q: %w", absPath, err)
		}
		tf.Hash = h
		return tf, nil
	}

	f, err := os.Open(absPath)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", absPath, err)
	}
	defer f.Close()

	h, err := hashFn(f)
	if err != nil {
		return nil, fmt.Errorf("hash %q: %w", absPath, err)
	}
	tf.Hash = h
	return tf, nil
}

func sha256Hash(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func cloneTF(tf *TrackedFile) *TrackedFile {
	c := *tf
	return &c
}
