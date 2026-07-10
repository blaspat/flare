// Package sync provides file-change tracking and chunked transfer for the Flare
// edge-mesh file-sync subsystem.
package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// --- Wire-protocol payloads ------------------------------------------------

// FileChangeAnnounce is broadcast when a tracked file is created or modified.
// It carries enough metadata for the receiver to decide whether to accept the
// file and how to reassemble it.
//
// To support backward-compatible content-defined chunking (CDC), the optional
// Chunks field holds a chunk index. When Chunks is non-nil the receiver treats
// the transfer as CDC mode (variable-size, sequential write); when Chunks is
// nil the receiver uses the traditional fixed-size mode (ChunkSize-based
// offset writes). Old clients that don't know about Chunks ignore the field
// via JSON omitempty and continue using the fixed-size path.
//
// The optional Clock field enables LWW CRDT-style merge: when present, the
// receiver compares it against its local vector clock to decide causality.
type FileChangeAnnounce struct {
	Path       string         `json:"path"`        // relative path within the watch tag
	Tag        string         `json:"tag"`         // watch-dir tag
	Size       int64          `json:"size"`        // total file size in bytes
	Hash       string         `json:"hash"`        // hex-encoded SHA-256 of the whole file
	Version    uint64         `json:"version"`     // tracker version
	NodeID     string         `json:"node_id"`     // originating node
	ChunkSize  int            `json:"chunk_size"`  // fixed chunk size (0 in CDC mode)
	ChunkCount int            `json:"chunk_count"` // total number of chunks
	ModTime    int64          `json:"mod_time"`    // unix-nano modification time
	Chunks     []CDCChunkMeta `json:"chunks,omitempty"` // CDC chunk index (nil = fixed-size mode)
	// DeltaIndices restricts data transfer to only the listed chunk indices.
	// When present and non-empty, only the chunks whose indices appear in this
	// list carry new data; the remaining chunks are unchanged and the receiver
	// may reconstruct them from its local copy. When nil or empty, all chunks
	// carry data (full transfer).
	DeltaIndices []int           `json:"delta,omitempty"`
	Clock        map[string]uint64 `json:"clock,omitempty"` // vector clock snapshot for LWW merge
}

// FileChunkPayload carries one chunk of file data.
// Data is base64-encoded; each chunk includes its index and the total count so
// the receiver can detect the final chunk and verify completeness.
type FileChunkPayload struct {
	Path       string `json:"path"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkCount int    `json:"chunk_count"`
	Data       string `json:"data"` // base64-encoded bytes
	Checksum   string `json:"checksum,omitempty"` // sha256 of this chunk (optional, for integrity)
	Version    uint64 `json:"version"`
}

// FileResumeRequest is sent by a receiver that missed some chunks.
// The sender responds by re-sending only the chunks listed in MissingIndices.
type FileResumeRequest struct {
	Path           string `json:"path"`
	Version        uint64 `json:"version"`
	MissingIndices []int  `json:"missing_indices"`
}

// SyncIndexEntry describes one file in a node's sync index.
type SyncIndexEntry struct {
	Path       string            `json:"path"`        // relative path within the watch tag
	Tag        string            `json:"tag"`         // watch-dir tag
	Size       int64             `json:"size"`        // -1 if deleted
	Hash       string            `json:"hash"`        // empty if deleted
	Version    uint64            `json:"version"`     // tracker version (global monotonic)
	ModTime    int64             `json:"mod_time"`    // unix-nano (0 if deleted)
	NodeID     string            `json:"node_id,omitempty"`  // originating node (for LWW)
	Clock      map[string]uint64 `json:"clock,omitempty"`    // vector clock snapshot (for LWW)
}

// SyncIndexPayload is exchanged when a new peer connects.
// It carries the sender's full file index so the receiver can reconcile.
type SyncIndexPayload struct {
	Files []SyncIndexEntry `json:"files"`
}

// SyncRequestPayload is sent when a node determines it needs a file
// from a peer after reconciling sync indexes.
type SyncRequestPayload struct {
	Files []SyncRequestEntry `json:"files"`
}

// SyncRequestEntry identifies a single file being requested.
type SyncRequestEntry struct {
	Path string `json:"path"`
	Tag  string `json:"tag"`
}

// FileChunk is the internal representation of a single file chunk.
type FileChunk struct {
	Index int
	Data  []byte
	Hash  string // SHA-256 of Data
}

// ChunkedFile is the result of chunking a file.
type ChunkedFile struct {
	Path       string
	Hash       string // SHA-256 of the original file
	Size       int64
	ChunkSize  int
	Chunks     []FileChunk
	ModTime    time.Time
}

// ChunkFile reads a file from disk, splits it into chunks of the given size,
// and returns a ChunkedFile with all metadata.
// When cm is non-nil and enabled, the file is transparently decrypted first.
func ChunkFile(path string, chunkSize int, cm *CryptoManager) (*ChunkedFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file: %q", path)
	}

	// When encryption is enabled, read and decrypt first, then chunk from memory.
	if cm != nil && cm.Enabled() {
		plain, err := cm.ReadDecryptedWithFallback(path)
		if err != nil {
			return nil, fmt.Errorf("read/decrypt %q: %w", path, err)
		}
		return chunkFromBytes(path, plain, chunkSize, info.ModTime()), nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	// Compute the full-file hash as we read.
	h := sha256.New()
	tee := io.TeeReader(f, h)

	var chunks []FileChunk
	buf := make([]byte, chunkSize)
	index := 0
	for {
		n, err := tee.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			ch := sha256.Sum256(data)
			chunks = append(chunks, FileChunk{
				Index: index,
				Data:  data,
				Hash:  hex.EncodeToString(ch[:]),
			})
			index++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
	}

	if len(chunks) == 0 {
		// Empty file — produce a single zero-length chunk.
		ch := sha256.Sum256(nil)
		chunks = append(chunks, FileChunk{
			Index: 0,
			Data:  []byte{},
			Hash:  hex.EncodeToString(ch[:]),
		})
	}

	fullHash := hex.EncodeToString(h.Sum(nil))

	return &ChunkedFile{
		Path:      path,
		Hash:      fullHash,
		Size:      info.Size(),
		ChunkSize: chunkSize,
		Chunks:    chunks,
		ModTime:   info.ModTime(),
	}, nil
}

// chunkFromBytes is an internal helper that chunks plaintext data without
// reading from disk. Used when encryption is enabled and we've already
// decrypted the file content.
func chunkFromBytes(path string, data []byte, chunkSize int, modTime time.Time) *ChunkedFile {
	h := sha256.New()
	r := bytes.NewReader(data)
	tee := io.TeeReader(r, h)

	var chunks []FileChunk
	buf := make([]byte, chunkSize)
	index := 0
	for {
		n, err := tee.Read(buf)
		if n > 0 {
			chunkData := make([]byte, n)
			copy(chunkData, buf[:n])
			ch := sha256.Sum256(chunkData)
			chunks = append(chunks, FileChunk{
				Index: index,
				Data:  chunkData,
				Hash:  hex.EncodeToString(ch[:]),
			})
			index++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			// Should not happen with a bytes.Reader.
			break
		}
	}

	if len(chunks) == 0 {
		ch := sha256.Sum256(nil)
		chunks = append(chunks, FileChunk{
			Index: 0,
			Data:  []byte{},
			Hash:  hex.EncodeToString(ch[:]),
		})
	}

	fullHash := hex.EncodeToString(h.Sum(nil))

	return &ChunkedFile{
		Path:      path,
		Hash:      fullHash,
		Size:      int64(len(data)),
		ChunkSize: chunkSize,
		Chunks:    chunks,
		ModTime:   modTime,
	}
}

// --- IncomingTransfer ------------------------------------------------------

// IncomingTransfer tracks the state of a file being received from a peer.
type IncomingTransfer struct {
	Path         string        // relative path within tag
	Tag          string        // watch-dir tag
	AbsPath      string        // where the final file will be written
	Size         int64
	Hash         string // expected full-file SHA-256
	Version      uint64
	ChunkSize    int
	ChunkCount   int
	Received     map[int]bool // chunk index → received
	NodeID       string       // originating peer
	StartedAt    time.Time
	LastActivity time.Time
	tempFile     *os.File

	// CDC mode fields. cdcChunks is non-nil when the transfer uses content-
	// defined chunking. In CDC mode the receiver writes data sequentially
	// (not at fixed offsets) and cdcWritten tracks progress.
	cdcChunks  []CDCChunkMeta
	cdcWritten int64
	// deltaIndices restricts expected data to only the listed indices when
	// non-nil (delta sync).
	deltaIndices []int

	// Clock carries the sender's vector clock snapshot from the announcement.
	// It is propagated to the tracker on finalizeTransfer for LWW merge.
	Clock      map[string]uint64
	FromNodeID string // node that sent the announcement (may differ from NodeID)
}

// IncomingTransferStore manages active incoming transfers.
type IncomingTransferStore struct {
	mu       sync.RWMutex
	entries  map[string]*IncomingTransfer // key: "path:version"
}

func NewIncomingTransferStore() *IncomingTransferStore {
	return &IncomingTransferStore{
		entries: make(map[string]*IncomingTransfer),
	}
}

func (its *IncomingTransferStore) key(path, nodeID string, version uint64) string {
	return fmt.Sprintf("%s:%s:%d", nodeID, path, version)
}

func (its *IncomingTransferStore) Get(path, nodeID string, version uint64) *IncomingTransfer {
	its.mu.RLock()
	defer its.mu.RUnlock()
	return its.entries[its.key(path, nodeID, version)]
}

func (its *IncomingTransferStore) Set(transfer *IncomingTransfer) {
	its.mu.Lock()
	defer its.mu.Unlock()
	its.entries[its.key(transfer.Path, transfer.NodeID, transfer.Version)] = transfer
}

func (its *IncomingTransferStore) Remove(path, nodeID string, version uint64) {
	its.mu.Lock()
	defer its.mu.Unlock()
	delete(its.entries, its.key(path, nodeID, version))
}

func (its *IncomingTransferStore) List() []*IncomingTransfer {
	its.mu.RLock()
	defer its.mu.RUnlock()
	out := make([]*IncomingTransfer, 0, len(its.entries))
	for _, v := range its.entries {
		out = append(out, v)
	}
	// Sort by started-at for deterministic output.
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}

// --- ConflictRecord ---------------------------------------------------------

// ConflictRecord describes a file conflict that was resolved by renaming the
// existing file to a conflict path.
type ConflictRecord struct {
	Path         string    `json:"path"`          // relative path within tag
	Tag          string    `json:"tag"`           // watch-dir tag
	AbsPath      string    `json:"abs_path"`      // original absolute path
	ConflictPath string    `json:"conflict_path"` // where the existing file was renamed
	IncomingNode string    `json:"incoming_node"` // node that sent the conflicting version
	IncomingHash string    `json:"incoming_hash"` // hash of the incoming (accepted) version
	LocalHash    string    `json:"local_hash"`    // hash of the original (renamed) version
	Timestamp    time.Time `json:"timestamp"`      // when the conflict was resolved
}

// --- TransferManager -------------------------------------------------------

// Broadcaster is the interface the mesh layer must satisfy so the transfer
// manager can send messages to all connected peers.
type Broadcaster interface {
	Broadcast(data []byte)
}

// TransferManager orchestrates chunked file transfer with resume support.
// It polls the FileTracker for changes, sends files to all peers, and
// accepts incoming file data to reassemble on disk.
type TransferManager struct {
	nodeID    string
	dataDir   string           // root for incoming file staging
	chunkSize int
	tracker   *FileTracker
	incoming  *IncomingTransferStore
	broadcast func(data []byte) // sends a raw message to all peers
	clock     *VectorClock      // node's own vector clock
	dirs      []WatchDir        // watch directories (for resolving absolute paths)
	clockMu   sync.Mutex

	throttler *Throttler // bandwidth limiter for outgoing chunks (nil = unlimited)

	conflicts   []ConflictRecord // records of resolved conflicts
	conflictMu  sync.RWMutex

	// CDC (content-defined chunking) state.
	cdcEnabled bool                     // when true, new sends use CDC chunking
	cdcAvgSize int                      // average CDC chunk size (default: chunkSize)
	cdcCache   map[string][]CDCChunkMeta // last-known CDC chunk index per absolute path
	cdcCacheMu sync.RWMutex

	cryptoMgr *CryptoManager // nil = encryption disabled
}

// NewTransferManager creates a transfer manager.
//   - nodeID: the local node name (used in wire protocol and vector clock)
//   - dataDir: root directory for staging incoming transfers
//   - chunkSize: max bytes per chunk (default 65536); also used as average CDC chunk size
//   - tracker: the file tracker (change detector)
//   - broadcast: function that sends a serialized Message to all peers
//   - dirs: watch directories (used to resolve relative paths)
//   - throttler: bandwidth limiter (nil = unlimited)
func NewTransferManager(
	nodeID string,
	dataDir string,
	chunkSize int,
	tracker *FileTracker,
	broadcast func(data []byte),
	dirs []WatchDir,
	throttler *Throttler,
) *TransferManager {
	if chunkSize <= 0 {
		chunkSize = 65536
	}
	return &TransferManager{
		nodeID:     nodeID,
		dataDir:    dataDir,
		chunkSize:  chunkSize,
		tracker:    tracker,
		incoming:   NewIncomingTransferStore(),
		broadcast:  broadcast,
		clock:      NewVectorClock(),
		dirs:       dirs,
		throttler:  throttler,
		cdcEnabled: true,     // enabled by default; can be disabled via WithCDCOption
		cdcAvgSize: chunkSize, // match existing chunk size for consistency
		cdcCache:   make(map[string][]CDCChunkMeta),
	}
}

// SetCryptoManager configures transparent encryption/decryption for files
// written to and read from disk. Pass nil to disable (default).
func (tm *TransferManager) SetCryptoManager(cm *CryptoManager) {
	tm.cryptoMgr = cm
}

// Poll scans all watched directories and broadcasts any changes to all peers.
// It should be called periodically (e.g. every 5 seconds).
func (tm *TransferManager) Poll() error {
	changes, err := tm.tracker.Scan()
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	for _, ch := range changes {
		switch ch.Type {
		case ChangeCreated, ChangeModified:
			if ch.File != nil {
				if err := tm.sendFile(ch.File); err != nil {
					slog.Warn("send file failed", "path", ch.Path, "err", err)
				}
			}
		case ChangeDeleted:
			tm.sendDelete(ch.Path, ch.Tag)
		}
	}
	return nil
}

// CleanStaleTransfers removes incoming transfers that have had no activity
// for the given duration. Returns the number of cleaned transfers.
func (tm *TransferManager) CleanStaleTransfers(timeout time.Duration) int {
	now := time.Now()
	var removed int
	for _, t := range tm.incoming.List() {
		if now.Sub(t.LastActivity) > timeout {
			tm.incoming.Remove(t.Path, t.NodeID, t.Version)
			if t.tempFile != nil {
				t.tempFile.Close()
				os.Remove(t.tempFile.Name())
			}
			slog.Debug("cleaned stale incoming transfer",
				"path", t.Path, "node", t.NodeID, "version", t.Version)
			removed++
		}
	}
	return removed
}

// HandleFileChange processes an incoming MsgFileChange announcement.
// It creates an IncomingTransfer entry and prepares to receive chunks.
// If the announcement has Size == -1, the file was deleted on the sender side
// and the local copy is removed.
//
// When a.Chunks is non-nil the transfer uses content-defined chunking (CDC).
// In CDC mode the temp file is not pre-allocated (sizes are variable) — chunks
// are written sequentially instead of at fixed offsets.
func (tm *TransferManager) HandleFileChange(from string, a *FileChangeAnnounce) {
	// Handle deletion signal.
	if a.Size == -1 {
		destPath := tm.resolveDestPath(a.Tag, a.Path)
		if destPath != "" {
			if err := os.Remove(destPath); err != nil && !os.IsNotExist(err) {
				slog.Warn("remove deleted file", "path", destPath, "err", err)
			} else {
				slog.Debug("removed file from deletion announcement",
					"path", a.Path, "from", from)
			}
		}
		return
	}
	// Resolve the destination path: find which watch dir matches the tag.
	destPath := tm.resolveDestPath(a.Tag, a.Path)
	if destPath == "" {
		slog.Warn("no matching watch dir for tag, rejecting file",
			"tag", a.Tag, "path", a.Path, "from", from)
		return
	}

	// If we already have this version, skip.
	existing := tm.incoming.Get(a.Path, a.NodeID, a.Version)
	if existing != nil {
		return
	}

	// --- LWW merge: compare vector clocks to determine causality ---
	// When the sender includes a vector clock, compare it against our local
	// tracked file's clock to decide whether this change should be accepted.
	// This enables CRDT-style Last-Writer-Wins merge for concurrent edits.
	if a.Clock != nil {
		localFile := tm.tracker.GetByTagAndPath(a.Tag, a.Path)
		if localFile != nil && !localFile.Deleted && localFile.Clock != nil {
			// If hashes match, content is identical — skip regardless of clock.
			if localFile.Hash == a.Hash {
				slog.Debug("LWW: file content identical, skipping",
					"path", a.Path, "from", from)
				return
			}

			incomingClock := FromMap(a.Clock)
			localClock := FromMap(localFile.Clock)
			rel := incomingClock.Compare(localClock)

			var shouldAccept bool
			switch rel {
			case HappenedAfter:
				shouldAccept = true // incoming is causally newer
			case HappenedBefore:
				shouldAccept = false // local is causally newer
			case Concurrent:
				// LWW tiebreaker: lexicographically lower node ID wins.
				shouldAccept = a.NodeID < localFile.LastWriter
			}

			if !shouldAccept {
				slog.Debug("LWW: rejected file change (local version wins)",
					"path", a.Path, "from", from,
					"relation", rel,
					"incoming_node", a.NodeID,
					"local_last_writer", localFile.LastWriter)
				return
			}

			slog.Debug("LWW: accepted file change",
				"path", a.Path, "from", from,
				"relation", rel,
				"incoming_node", a.NodeID,
				"local_last_writer", localFile.LastWriter)
		}
	}

	isCDC := len(a.Chunks) > 0

	// Create temp file for assembly.
	tmpDir := filepath.Join(tm.dataDir, ".incoming")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		slog.Error("create incoming dir", "err", err)
		return
	}
	tmpFile, err := os.CreateTemp(tmpDir, "transfer-*")
	if err != nil {
		slog.Error("create temp file", "err", err)
		return
	}

	// Pre-allocate the file only for fixed-size mode (CDC chunks vary in size).
	if !isCDC && a.Size > 0 {
		if err := tmpFile.Truncate(a.Size); err != nil {
			slog.Warn("pre-allocate temp file", "err", err)
		}
	}

	t := &IncomingTransfer{
		Path:         a.Path,
		Tag:          a.Tag,
		AbsPath:      destPath,
		Size:         a.Size,
		Hash:         a.Hash,
		Version:      a.Version,
		ChunkSize:    a.ChunkSize,
		ChunkCount:   a.ChunkCount,
		Received:     make(map[int]bool),
		NodeID:       a.NodeID,
		StartedAt:    time.Now(),
		LastActivity: time.Now(),
		tempFile:     tmpFile,
		cdcChunks:    a.Chunks,
		deltaIndices: a.DeltaIndices,
		Clock:        a.Clock,
		FromNodeID:   from,
	}
	tm.incoming.Set(t)

	if isCDC {
		slog.Debug("incoming CDC file transfer started",
			"path", a.Path, "from", from, "size", a.Size, "cdc_chunks", len(a.Chunks))
	} else {
		slog.Debug("incoming file transfer started",
			"path", a.Path, "from", from, "size", a.Size, "chunks", a.ChunkCount)
	}
}

// HandleFileChunk processes an incoming file chunk, writing it to the temp
// file at the correct offset (fixed-size mode) or sequentially (CDC mode).
// If all chunks are received, the file is verified and moved to its final location.
func (tm *TransferManager) HandleFileChunk(from string, c *FileChunkPayload) {
	t := tm.incoming.Get(c.Path, from, c.Version)
	if t == nil {
		slog.Warn("received chunk for unknown transfer",
			"path", c.Path, "from", from, "version", c.Version)
		return
	}

	// Decode chunk data.
	data, err := base64.StdEncoding.DecodeString(c.Data)
	if err != nil {
		slog.Error("decode chunk data", "path", c.Path, "index", c.ChunkIndex, "err", err)
		return
	}

	if t.cdcChunks != nil {
		// CDC mode: write at the chunk's content-defined offset.
		// Use offset from the CDC meta so chunks can arrive in any order.
		if c.ChunkIndex < 0 || c.ChunkIndex >= len(t.cdcChunks) {
			slog.Error("CDC chunk index out of range",
				"path", c.Path, "index", c.ChunkIndex, "total_cdc", len(t.cdcChunks))
			return
		}
		expected := t.cdcChunks[c.ChunkIndex]
		if _, err := t.tempFile.WriteAt(data, expected.Offset); err != nil {
			slog.Error("write CDC chunk to temp file", "path", c.Path, "index", c.ChunkIndex, "err", err)
			return
		}
	} else {
		// Fixed-size mode: write at the correct offset.
		offset := int64(c.ChunkIndex) * int64(t.ChunkSize)
		if _, err := t.tempFile.WriteAt(data, offset); err != nil {
			slog.Error("write chunk to temp file", "path", c.Path, "index", c.ChunkIndex, "err", err)
			return
		}
	}

	t.Received[c.ChunkIndex] = true
	t.LastActivity = time.Now()

	// Check if all chunks received.
	if len(t.Received) == t.ChunkCount {
		tm.finalizeTransfer(t)
	}
}

// HandleFileResume processes an incoming resume request.
// It re-sends only the chunks that were missed.
func (tm *TransferManager) HandleFileResume(from string, req *FileResumeRequest) {
	// Build a lookup of missing indices.
	missing := make(map[int]bool, len(req.MissingIndices))
	for _, idx := range req.MissingIndices {
		missing[idx] = true
	}
	if len(missing) == 0 {
		return
	}

	// Find the tracked file.
	absPath := tm.resolveDestPath("", req.Path)
	if absPath == "" {
		absPath = req.Path
	}
	cf, err := ChunkFile(absPath, tm.chunkSize, tm.cryptoMgr)
	if err != nil {
		slog.Warn("resume: cannot read file", "path", req.Path, "err", err)
		return
	}

	// Send only missing chunks with bandwidth throttling.
	ctx := context.Background()
	for _, chunk := range cf.Chunks {
		if !missing[chunk.Index] {
			continue
		}
		if tm.throttler != nil {
			if err := tm.throttler.WaitN(ctx, len(chunk.Data)); err != nil {
				slog.Warn("resume throttle interrupted", "path", req.Path, "err", err)
				return
			}
		}
		payload := &FileChunkPayload{
			Path:       req.Path,
			ChunkIndex: chunk.Index,
			ChunkCount: len(cf.Chunks),
			Data:       base64.StdEncoding.EncodeToString(chunk.Data),
			Checksum:   chunk.Hash,
			Version:    req.Version,
		}
		tm.sendMsg("file_chunk", payload)
	}
}

// sendFile chunks a tracked file and sends it to all peers.
func (tm *TransferManager) sendFile(tf *TrackedFile) error {
	if tm.cdcEnabled {
		return tm.sendFileCDC(tf)
	}
	return tm.sendFileFixed(tf)
}

// sendFileFixed chunks a tracked file and sends it using fixed-size chunking.
// This is the legacy path, retained for backward compatibility.
func (tm *TransferManager) sendFileFixed(tf *TrackedFile) error {
	cf, err := ChunkFile(tf.Path, tm.chunkSize, tm.cryptoMgr)
	if err != nil {
		return fmt.Errorf("chunk %q: %w", tf.Path, err)
	}

	// Determine the relative path within the watch tag.
	relPath := tm.relativePath(tf.Path, tf.Tag)

	// Bump the local vector clock and snapshot.
	tm.clockMu.Lock()
	version := tm.clock.Increment(tm.nodeID)
	clockSnap := tm.clock.All()
	tm.clockMu.Unlock()

	// Update the tracker's clock info so the file's causal state is persisted.
	tm.tracker.SetFileClock(tf.Path, tm.nodeID, clockSnap)

	// Send the announcement.
	announce := &FileChangeAnnounce{
		Path:       relPath,
		Tag:        tf.Tag,
		Size:       cf.Size,
		Hash:       cf.Hash,
		Version:    version,
		NodeID:     tm.nodeID,
		ChunkSize:  tm.chunkSize,
		ChunkCount: len(cf.Chunks),
		ModTime:    cf.ModTime.UnixNano(),
		Clock:      clockSnap,
	}
	tm.sendMsg("file_change", announce)

	// Send each chunk with bandwidth throttling.
	ctx := context.Background()
	for _, chunk := range cf.Chunks {
		if tm.throttler != nil {
			if err := tm.throttler.WaitN(ctx, len(chunk.Data)); err != nil {
				return fmt.Errorf("throttle interrupted: %w", err)
			}
		}
		payload := &FileChunkPayload{
			Path:       relPath,
			ChunkIndex: chunk.Index,
			ChunkCount: len(cf.Chunks),
			Data:       base64.StdEncoding.EncodeToString(chunk.Data),
			Checksum:   chunk.Hash,
			Version:    version,
		}
		tm.sendMsg("file_chunk", payload)
	}

	return nil
}

// sendFileCDC sends a file using content-defined chunking (CDC) with delta
// sync. When the file has been sent before, unchanged chunks (matching cached
// hashes) are skipped and the receiver is told which indices carry new data.
func (tm *TransferManager) sendFileCDC(tf *TrackedFile) error {
	relPath := tm.relativePath(tf.Path, tf.Tag)

	// Chunk the file with CDC.
	res, err := ChunkFileCDC(tf.Path, tm.cdcAvgSize, tm.cryptoMgr)
	if err != nil {
		return fmt.Errorf("cdc chunk %q: %w", tf.Path, err)
	}

	// Bump the local vector clock and snapshot.
	tm.clockMu.Lock()
	version := tm.clock.Increment(tm.nodeID)
	clockSnap := tm.clock.All()
	tm.clockMu.Unlock()

	// Update the tracker's clock info for LWW merge.
	tm.tracker.SetFileClock(tf.Path, tm.nodeID, clockSnap)

	// Delta sync: compare against cached chunk metas.
	var deltaIndices []int

	tm.cdcCacheMu.RLock()
	cached, hasCached := tm.cdcCache[tf.Path]
	tm.cdcCacheMu.RUnlock()

	if hasCached && len(cached) == len(res.Meta) {
		// Same number of chunks — compare hashes for delta sync.
		for i := range res.Meta {
			if i < len(cached) && cached[i].Hash != res.Meta[i].Hash {
				deltaIndices = append(deltaIndices, i)
			}
		}
		// If all chunks changed or almost all, it's not worth delta-syncing;
		// fall back to full send.
		if len(deltaIndices) > len(res.Meta)*3/4 {
			deltaIndices = nil // send everything
		}
	}

	// Build the announcement.
	announce := &FileChangeAnnounce{
		Path:         relPath,
		Tag:          tf.Tag,
		Size:         res.Size,
		Hash:         res.Hash,
		Version:      version,
		NodeID:       tm.nodeID,
		ChunkSize:    0, // signals CDC mode to new receivers
		ChunkCount:   len(res.Meta),
		ModTime:      tf.ModTime.UnixNano(),
		Chunks:       res.Meta,
		DeltaIndices: deltaIndices, // nil = send all, non-nil = send only these
		Clock:        clockSnap,
	}
	tm.sendMsg("file_change", announce)

	// Cache the metas for future delta sync.
	tm.cdcCacheMu.Lock()
	tm.cdcCache[tf.Path] = res.Meta
	tm.cdcCacheMu.Unlock()

	// Determine which chunks need data sent.
	isDelta := len(deltaIndices) > 0

	// Build a set for fast lookup.
	var needSend map[int]bool
	if isDelta {
		needSend = make(map[int]bool, len(deltaIndices))
		for _, idx := range deltaIndices {
			needSend[idx] = true
		}
	}

	ctx := context.Background()
	for _, ch := range res.Chunks {
		if isDelta && !needSend[ch.Index] {
			continue // skip unchanged chunk
		}
		if tm.throttler != nil {
			if err := tm.throttler.WaitN(ctx, len(ch.Data)); err != nil {
				return fmt.Errorf("throttle interrupted: %w", err)
			}
		}
		payload := &FileChunkPayload{
			Path:       relPath,
			ChunkIndex: ch.Index,
			ChunkCount: len(res.Chunks),
			Data:       base64.StdEncoding.EncodeToString(ch.Data),
			Checksum:   ch.Hash,
			Version:    version,
		}
		tm.sendMsg("file_chunk", payload)
	}

	slog.Debug("sent file via CDC",
		"path", relPath,
		"size", res.Size,
		"chunks", len(res.Chunks),
		"delta", isDelta,
		"delta_count", len(deltaIndices),
	)
	return nil
}

// sendDelete notifies peers that a file was deleted.
func (tm *TransferManager) sendDelete(path, tag string) {
	relPath := tm.relativePath(path, tag)
	tm.clockMu.Lock()
	version := tm.clock.Increment(tm.nodeID)
	clockSnap := tm.clock.All()
	tm.clockMu.Unlock()

	// Update tracker clock for the deleted file.
	tm.tracker.SetFileClock(path, tm.nodeID, clockSnap)

	announce := &FileChangeAnnounce{
		Path:    relPath,
		Tag:     tag,
		Size:    -1, // signals deletion
		Hash:    "",
		Version: version,
		NodeID:  tm.nodeID,
		Clock:   clockSnap,
	}
	tm.sendMsg("file_change", announce)
}

// finalizeTransfer verifies the assembled file, moves it to the final
// location, and removes the transfer state.
func (tm *TransferManager) finalizeTransfer(t *IncomingTransfer) {
	defer func() {
		t.tempFile.Close()
		os.Remove(t.tempFile.Name())
		tm.incoming.Remove(t.Path, t.NodeID, t.Version)
	}()

	// Verify the full-file hash.
	if _, err := t.tempFile.Seek(0, io.SeekStart); err != nil {
		slog.Error("seek temp file for hash verification", "path", t.Path, "err", err)
		return
	}
	h := sha256.New()
	if _, err := io.Copy(h, t.tempFile); err != nil {
		slog.Error("hash temp file", "path", t.Path, "err", err)
		return
	}
	gotHash := hex.EncodeToString(h.Sum(nil))
	if gotHash != t.Hash {
		slog.Warn("file hash mismatch, requesting resume",
			"path", t.Path, "expected", t.Hash, "got", gotHash)
		// Request missing chunks.
		missing := tm.findMissingChunks(t)
		if len(missing) > 0 {
			tm.sendMsg("file_resume", &FileResumeRequest{
				Path:           t.Path,
				Version:        t.Version,
				MissingIndices: missing,
			})
		}
		return
	}

	// Ensure the destination directory exists.
	if err := os.MkdirAll(filepath.Dir(t.AbsPath), 0755); err != nil {
		slog.Error("create destination dir", "path", filepath.Dir(t.AbsPath), "err", err)
		return
	}

	// --- Conflict detection ---
	// If the destination file already exists with different content, rename it
	// to a conflict path before writing the incoming version.
	if existingInfo, statErr := os.Stat(t.AbsPath); statErr == nil && existingInfo.Mode().IsRegular() {
		existingHash, hashErr := hashFileContent(t.AbsPath, tm.cryptoMgr)
		if hashErr != nil {
			slog.Warn("conflict check: cannot hash existing file, overwriting anyway",
				"path", t.Path, "err", hashErr)
		} else if existingHash == t.Hash {
			// Content is identical — nothing to do. The temp file was already
			// written with the same content. Clean it up and return.
			slog.Debug("file unchanged (identical hash), skipping write",
				"path", t.Path)
			return
		} else {
			conflictPath := fmt.Sprintf("%s.conflict.%s.%d",
				t.AbsPath, t.NodeID, time.Now().Unix())
			if err := os.Rename(t.AbsPath, conflictPath); err != nil {
				slog.Error("conflict: failed to rename existing file, overwriting",
					"path", t.AbsPath, "err", err)
			} else {
				slog.Warn("file conflict detected — renamed existing file",
					"path", t.Path,
					"local_hash", existingHash,
					"incoming_hash", t.Hash,
					"conflict_path", conflictPath)

				tm.addConflict(ConflictRecord{
					Path:         t.Path,
					Tag:          t.Tag,
					AbsPath:      t.AbsPath,
					ConflictPath: conflictPath,
					IncomingNode: t.NodeID,
					IncomingHash: t.Hash,
					LocalHash:    existingHash,
					Timestamp:    time.Now(),
				})
			}
		}
	}

	// Move temp file to final location, optionally encrypting.
	if tm.cryptoMgr != nil && tm.cryptoMgr.Enabled() {
		// Read plaintext from temp, encrypt, write to destination.
		if _, err := t.tempFile.Seek(0, io.SeekStart); err != nil {
			slog.Error("seek temp for encryption", "path", t.Path, "err", err)
			return
		}
		plain, err := io.ReadAll(t.tempFile)
		if err != nil {
			slog.Error("read temp for encryption", "path", t.Path, "err", err)
			return
		}
		if err := tm.cryptoMgr.WriteEncrypted(t.AbsPath, plain); err != nil {
			slog.Error("write encrypted file", "path", t.AbsPath, "err", err)
			return
		}
	} else if err := os.Rename(t.tempFile.Name(), t.AbsPath); err != nil {
		// Fallback: copy and delete.
		src, err := os.Open(t.tempFile.Name())
		if err != nil {
			slog.Error("open temp for copy", "err", err)
			return
		}
		defer src.Close()

		dst, err := os.Create(t.AbsPath)
		if err != nil {
			slog.Error("create destination file", "path", t.AbsPath, "err", err)
			return
		}
		defer dst.Close()

		if _, err := io.Copy(dst, src); err != nil {
			slog.Error("copy to destination", "err", err)
			return
		}
	}

	// Propagate the vector clock to the tracker for LWW merge state.
	if t.Clock != nil && t.AbsPath != "" {
		tm.tracker.SetFileClock(t.AbsPath, t.FromNodeID, t.Clock)
	}

	slog.Info("file transfer complete", "path", t.Path, "size", t.Size)
}

// findMissingChunks returns the indices of chunks that haven't been received.
func (tm *TransferManager) findMissingChunks(t *IncomingTransfer) []int {
	var missing []int
	for i := 0; i < t.ChunkCount; i++ {
		if !t.Received[i] {
			missing = append(missing, i)
		}
	}
	return missing
}

// sendMsg creates a Message and broadcasts it to all peers.
func (tm *TransferManager) sendMsg(msgType string, payload any) {
	// We build the JSON manually to avoid a dependency on the mesh package.
	// This matches the mesh.Message wire format.
	type msg struct {
		Type    string `json:"type"`
		From    string `json:"from"`
		SentAt  int64  `json:"sent_at"`
		Payload any    `json:"payload,omitempty"`
	}
	m := msg{
		Type:    msgType,
		From:    tm.nodeID,
		SentAt:  time.Now().UnixNano(),
		Payload: payload,
	}
	data, err := json.Marshal(m)
	if err != nil {
		slog.Error("marshal message", "type", msgType, "err", err)
		return
	}
	tm.broadcast(data)
}

// resolveDestPath converts a relative path within a tag to an absolute
// filesystem path using the watch directory configuration.
func (tm *TransferManager) resolveDestPath(tag, relPath string) string {
	for _, d := range tm.dirs {
		if d.Tag == tag {
			return filepath.Join(d.Path, relPath)
		}
	}
	// Fallback: use data dir.
	return filepath.Join(tm.dataDir, tag, relPath)
}

// relativePath strips the watch directory prefix from an absolute path to
// produce a relative path, using the tag's directory as the root.
func (tm *TransferManager) relativePath(absPath, tag string) string {
	for _, d := range tm.dirs {
		if d.Tag == tag {
			rel, err := filepath.Rel(d.Path, absPath)
			if err == nil {
				return rel
			}
		}
	}
	return filepath.Base(absPath)
}

// BuildSyncIndex builds the full file index from the tracker's current state.
// Non-deleted files carry their hash/size/modtime; deleted files have Size=-1.
// When available, vector clock info is included for LWW merge reconciliation.
func (tm *TransferManager) BuildSyncIndex() *SyncIndexPayload {
	snap := tm.tracker.Snapshot()
	entries := make([]SyncIndexEntry, 0, len(snap))
	for _, tf := range snap {
		entry := SyncIndexEntry{
			Path:    tm.relativePath(tf.Path, tf.Tag),
			Tag:     tf.Tag,
			Version: tf.Version,
			NodeID:  tf.LastWriter,
			Clock:   tf.Clock,
		}
		if tf.Deleted {
			entry.Size = -1
		} else {
			entry.Size = tf.Size
			entry.Hash = tf.Hash
			entry.ModTime = tf.ModTime.UnixNano()
		}
		entries = append(entries, entry)
	}
	return &SyncIndexPayload{Files: entries}
}

// HandleSyncIndex reconciles a peer's file index against our local state.
// It returns a SyncRequestPayload listing files we need to fetch from the peer.
func (tm *TransferManager) HandleSyncIndex(from string, index *SyncIndexPayload) *SyncRequestPayload {
	var requests []SyncRequestEntry

	for _, entry := range index.Files {
		if from == tm.nodeID {
			continue // skip self
		}

		local := tm.tracker.GetByTagAndPath(entry.Tag, entry.Path)
		destPath := tm.resolveDestPath(entry.Tag, entry.Path)

		if entry.Size == -1 {
			// Peer has this file as deleted.
			if local != nil && !local.Deleted {
				// We still have it — delete ours.
				if err := os.Remove(destPath); err != nil && !os.IsNotExist(err) {
					slog.Warn("sync-index: remove file deleted by peer",
						"path", entry.Path, "tag", entry.Tag, "err", err)
				}
				slog.Info("sync-index: deleted file (peer had tombstone)",
					"path", entry.Path, "from", from)
			}
			continue
		}

		// Peer has a live file.
		if local == nil || local.Deleted {
			// We don't have it — request it.
			requests = append(requests, SyncRequestEntry{Path: entry.Path, Tag: entry.Tag})
			continue
		}

		// Both have the file. Use vector clock comparison when available.
		if local.Clock != nil && entry.Clock != nil {
			// LWW via vector clocks.
			localVC := FromMap(local.Clock)
			entryVC := FromMap(entry.Clock)
			rel := entryVC.Compare(localVC)

			var peerWins bool
			switch rel {
			case HappenedAfter:
				peerWins = true // peer is causally ahead
			case HappenedBefore:
				peerWins = false // we are causally ahead
			case Concurrent:
				// LWW tiebreaker: lower node ID wins.
				peerWins = entry.NodeID < local.LastWriter
			}

			if peerWins && entry.Hash != local.Hash {
				requests = append(requests, SyncRequestEntry{Path: entry.Path, Tag: entry.Tag})
			}
		} else {
			// Fall back to version + modtime comparison (backward compatible).
			if entry.Version > local.Version || (entry.Version == local.Version && entry.ModTime > local.ModTime.UnixNano()) {
				// Peer has a newer version (or same version but newer mtime).
				// Only request if the content differs.
				if entry.Hash != local.Hash {
					requests = append(requests, SyncRequestEntry{Path: entry.Path, Tag: entry.Tag})
				}
			}
		}
	}

	if len(requests) == 0 {
		return nil
	}
	return &SyncRequestPayload{Files: requests}
}

// HandleSyncRequest processes a file request from a peer.
// For each requested file, it reads the file from disk and sends it.
func (tm *TransferManager) HandleSyncRequest(from string, req *SyncRequestPayload) {
	for _, f := range req.Files {
		absPath := tm.resolveDestPath(f.Tag, f.Path)
		if absPath == "" {
			slog.Warn("sync-request: unknown tag", "tag", f.Tag, "path", f.Path)
			continue
		}
		tf := tm.tracker.GetByTagAndPath(f.Tag, f.Path)
		if tf == nil || tf.Deleted {
			slog.Debug("sync-request: file not tracked locally", "path", f.Path)
			continue
		}
		if err := tm.sendFile(tf); err != nil {
			slog.Warn("sync-request: send file failed", "path", f.Path, "err", err)
		} else {
			slog.Info("sync-request: sent file to peer", "path", f.Path, "to", from)
		}
	}
}

// --- Conflict management ----------------------------------------------------

// Conflicts returns a copy of all recorded conflict records.
func (tm *TransferManager) Conflicts() []ConflictRecord {
	tm.conflictMu.RLock()
	defer tm.conflictMu.RUnlock()
	out := make([]ConflictRecord, len(tm.conflicts))
	copy(out, tm.conflicts)
	return out
}

// addConflict appends a conflict record.
func (tm *TransferManager) addConflict(r ConflictRecord) {
	tm.conflictMu.Lock()
	defer tm.conflictMu.Unlock()
	tm.conflicts = append(tm.conflicts, r)
}

// hashFileContent reads a file and returns its SHA-256 hex digest.
// When cm is non-nil and enabled, the file is transparently decrypted first.
func hashFileContent(absPath string, cm *CryptoManager) (string, error) {
	if cm != nil && cm.Enabled() {
		plain, err := cm.ReadDecryptedWithFallback(absPath)
		if err != nil {
			return "", err
		}
		h := sha256.Sum256(plain)
		return hex.EncodeToString(h[:]), nil
	}
	f, err := os.Open(absPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
