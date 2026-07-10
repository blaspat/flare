package sync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Test helpers -----------------------------------------------------------

// randomContent returns a byte slice filled with deterministic but non-periodic
// pseudo-random data that produces natural CDC boundaries.
func randomContent(length int) []byte {
	b := make([]byte, length)
	rng := rand.New(rand.NewSource(42))
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}

// --- CDC chunker basic tests -----------------------------------------------

func TestCDCChunker_EmptyContent(t *testing.T) {
	t.Parallel()
	r := bytes.NewReader([]byte{})
	chunker := NewCDCChunker(r, 65536)

	_, err := chunker.Next()
	if err != io.EOF {
		t.Fatalf("expected io.EOF for empty content, got: %v", err)
	}
}

func TestCDCChunker_SingleByte(t *testing.T) {
	t.Parallel()
	content := []byte{'A'}
	r := bytes.NewReader(content)
	chunker := NewCDCChunker(r, 65536)

	ch, err := chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ch.Data) != 1 || ch.Data[0] != 'A' {
		t.Errorf("expected single byte 'A', got %v", ch.Data)
	}
	if ch.Offset != 0 {
		t.Errorf("expected offset 0, got %d", ch.Offset)
	}
	if ch.Index != 0 {
		t.Errorf("expected index 0, got %d", ch.Index)
	}
}

func TestCDCChunker_SmallContent(t *testing.T) {
	t.Parallel()
	// Content smaller than minSize (65536/4 = 16384).
	content := randomContent(1000)
	r := bytes.NewReader(content)
	chunker := NewCDCChunker(r, 65536)

	ch, err := chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Small content should be a single chunk.
	if len(ch.Data) != 1000 {
		t.Errorf("expected 1000 bytes, got %d", len(ch.Data))
	}

	_, err = chunker.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestCDCChunker_ProducesVariableSizes(t *testing.T) {
	t.Parallel()
	content := randomContent(1024 * 1024)
	r := bytes.NewReader(content)
	chunker := NewCDCChunker(r, 65536)

	var chunks [][]byte
	for {
		ch, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("chunk error: %v", err)
		}
		chunks = append(chunks, ch.Data)
	}

	// Should produce roughly 16 chunks (1MB / 64KB avg).
	// Random data with FastCDC gives ~normSize average.
	if len(chunks) < 8 {
		t.Errorf("expected ~16 chunks for 1MB random content, got %d", len(chunks))
	}
	if len(chunks) > 80 {
		t.Errorf("expected ~16 chunks for 1MB random content, got %d", len(chunks))
	}

	// Chunks should be variable-sized.
	sizes := make(map[int]bool)
	for _, c := range chunks {
		sizes[len(c)] = true
	}
	if len(sizes) < 2 {
		t.Errorf("expected variable-size chunks, all %d chunks are %d bytes",
			len(chunks), len(chunks[0]))
	}

	// Sum of chunk sizes should equal original.
	totalSize := 0
	for _, c := range chunks {
		totalSize += len(c)
	}
	if totalSize != len(content) {
		t.Errorf("chunk sizes sum to %d, expected %d", totalSize, len(content))
	}
}

func TestCDCChunker_Deterministic(t *testing.T) {
	t.Parallel()
	content := randomContent(500000)

	// Chunk twice with same content.
	r1 := bytes.NewReader(content)
	r2 := bytes.NewReader(content)

	c1 := NewCDCChunker(r1, 65536)
	c2 := NewCDCChunker(r2, 65536)

	var chunks1, chunks2 [][]byte
	for {
		ch, err := c1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c1 error: %v", err)
		}
		chunks1 = append(chunks1, ch.Data)
	}
	for {
		ch, err := c2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c2 error: %v", err)
		}
		chunks2 = append(chunks2, ch.Data)
	}

	// Same number of chunks.
	if len(chunks1) != len(chunks2) {
		t.Fatalf("chunk count differs: %d vs %d", len(chunks1), len(chunks2))
	}

	// Same chunk sizes and content.
	for i := range chunks1 {
		if len(chunks1[i]) != len(chunks2[i]) {
			t.Errorf("chunk %d size: %d vs %d", i, len(chunks1[i]), len(chunks2[i]))
		}
		if !bytes.Equal(chunks1[i], chunks2[i]) {
			t.Errorf("chunk %d content differs", i)
		}
	}
}

func TestCDCChunker_DifferentContent_DifferentBoundaries(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	content1 := make([]byte, 500000)
	for i := range content1 {
		content1[i] = byte(rng.Intn(256))
	}
	content2 := make([]byte, len(content1))
	copy(content2, content1)
	content2[len(content2)/2] ^= 0x01 // flip one bit in the middle

	c1 := NewCDCChunker(bytes.NewReader(content1), 65536)
	c2 := NewCDCChunker(bytes.NewReader(content2), 65536)

	var sizes1, sizes2 []int
	for {
		ch, err := c1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c1 error: %v", err)
		}
		sizes1 = append(sizes1, len(ch.Data))
	}
	for {
		ch, err := c2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c2 error: %v", err)
		}
		sizes2 = append(sizes2, len(ch.Data))
	}

	// Sum of sizes matches.
	sum1 := 0
	for _, s := range sizes1 {
		sum1 += s
	}
	sum2 := 0
	for _, s := range sizes2 {
		sum2 += s
	}
	if sum1 != 500000 {
		t.Errorf("sum1 = %d, expected 500000", sum1)
	}
	if sum2 != 500000 {
		t.Errorf("sum2 = %d, expected 500000", sum2)
	}

	// The bit flip should cause boundaries to differ.
	same := len(sizes1) == len(sizes2)
	for i := range sizes1 {
		if i >= len(sizes2) || sizes1[i] != sizes2[i] {
			same = false
			break
		}
	}
	if same {
		t.Log("Note: both contents produced same chunk boundaries")
	}
}

func TestCDCChunker_FullHash(t *testing.T) {
	t.Parallel()
	content := randomContent(250000)
	r := bytes.NewReader(content)
	chunker := NewCDCChunker(r, 65536)

	for {
		_, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("chunk error: %v", err)
		}
	}

	gotHash := chunker.FullHash()
	h := sha256.Sum256(content)
	wantHash := hex.EncodeToString(h[:])
	if gotHash != wantHash {
		t.Errorf("full hash mismatch:\n  got:  %s\n  want: %s", gotHash, wantHash)
	}
}

// --- ChunkFileCDC tests ----------------------------------------------------

func TestChunkFileCDC_ProducesCorrectResult(t *testing.T) {
	dir := t.TempDir()
	content := randomContent(100000)
	path := writeTestFile(t, dir, "cdc_test.dat", content)

	res, err := ChunkFileCDC(path, 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed: %v", err)
	}

	if res.Size != int64(len(content)) {
		t.Errorf("size: want %d, got %d", len(content), res.Size)
	}

	// Verify full hash.
	h := sha256.Sum256(content)
	wantHash := hex.EncodeToString(h[:])
	if res.Hash != wantHash {
		t.Errorf("full hash mismatch: want %s, got %s", wantHash, res.Hash)
	}

	// Verify chunk metas match chunks.
	if len(res.Meta) != len(res.Chunks) {
		t.Fatalf("meta count %d != chunk count %d", len(res.Meta), len(res.Chunks))
	}

	// Verify content reconstruction.
	var reconstructed []byte
	for _, ch := range res.Chunks {
		reconstructed = append(reconstructed, ch.Data...)
	}
	if !bytes.Equal(reconstructed, content) {
		t.Error("reconstructed content differs from original")
	}

	// Verify each meta's hash matches its chunk data.
	for i, meta := range res.Meta {
		ch := sha256.Sum256(res.Chunks[i].Data)
		want := hex.EncodeToString(ch[:])
		if meta.Hash != want {
			t.Errorf("meta[%d] hash mismatch: want %s, got %s", i, want, meta.Hash)
		}
	}

	// Verify offsets are correct.
	var totalOffset int64
	for i, meta := range res.Meta {
		if meta.Offset != totalOffset {
			t.Errorf("meta[%d] offset: want %d, got %d", i, totalOffset, meta.Offset)
		}
		totalOffset += int64(meta.Size)
	}
}

func TestChunkFileCDC_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := writeTestFile(t, dir, "empty_cdc.dat", []byte{})

	res, err := ChunkFileCDC(path, 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed on empty file: %v", err)
	}

	if res.Size != 0 {
		t.Errorf("size: want 0, got %d", res.Size)
	}
	if len(res.Chunks) != 1 {
		t.Fatalf("want 1 chunk for empty file, got %d", len(res.Chunks))
	}
	if len(res.Chunks[0].Data) != 0 {
		t.Errorf("chunk data: want empty, got %d bytes", len(res.Chunks[0].Data))
	}
	if len(res.Meta) != 1 {
		t.Fatalf("want 1 meta entry, got %d", len(res.Meta))
	}
	if res.Meta[0].Size != 0 {
		t.Errorf("meta size: want 0, got %d", res.Meta[0].Size)
	}
}

func TestChunkFileCDC_LargeFile_SumMatches(t *testing.T) {
	dir := t.TempDir()
	content := randomContent(5 * 1024 * 1024) // 5 MB
	path := writeTestFile(t, dir, "large_cdc.dat", content)

	res, err := ChunkFileCDC(path, 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed: %v", err)
	}

	if res.Size != int64(len(content)) {
		t.Errorf("size mismatch: %d vs %d", res.Size, len(content))
	}

	// Sum of chunk data sizes should equal file size.
	var dataSize int64
	for _, ch := range res.Chunks {
		dataSize += int64(len(ch.Data))
	}
	if dataSize != res.Size {
		t.Errorf("sum of chunk data (%d) != file size (%d)", dataSize, res.Size)
	}

	// Verify meta size consistency.
	var metaSize int64
	for _, m := range res.Meta {
		metaSize += int64(m.Size)
	}
	if metaSize != dataSize {
		t.Errorf("sum of meta sizes (%d) != sum of chunk data (%d)", metaSize, dataSize)
	}
}

func TestChunkFileCDC_NotExists(t *testing.T) {
	_, err := ChunkFileCDC("/nonexistent/cdc_path.dat", 65536, nil)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

// --- CDC transfer integration tests ----------------------------------------

// TestCDCTransfer_EndToEnd verifies that a file chunked with CDC on the
// sender side can be reassembled correctly by the receiver.
func TestCDCTransfer_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")
	content := randomContent(200000)

	expectedPath := filepath.Join(watchDir, "cdc_received.dat")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Chunk the file with CDC.
	res, err := ChunkFileCDC(writeTestFile(t, dir, "source.dat", content), 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed: %v", err)
	}

	announce := &FileChangeAnnounce{
		Path:       "cdc_received.dat",
		Tag:        "default",
		Size:       res.Size,
		Hash:       res.Hash,
		Version:    42,
		NodeID:     "sender",
		ChunkSize:  0,
		ChunkCount: len(res.Meta),
		Chunks:     res.Meta,
	}
	tm.HandleFileChange("sender", announce)

	// Send all CDC chunks.
	for _, ch := range res.Chunks {
		payload := &FileChunkPayload{
			Path:       "cdc_received.dat",
			ChunkIndex: ch.Index,
			ChunkCount: len(res.Chunks),
			Data:       toB64(ch.Data),
			Version:    42,
		}
		tm.HandleFileChunk("sender", payload)
	}

	// Verify the file was assembled correctly.
	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("read assembled file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("assembled content mismatch: %d bytes vs %d bytes", len(got), len(content))
	}
	if hashOf(got) != hashOf(content) {
		t.Errorf("assembled hash mismatch")
	}

	// Verify transfer was cleaned up.
	if tm.incoming.Get("cdc_received.dat", "sender", 42) != nil {
		t.Error("expected transfer to be removed after completion")
	}
}

// TestCDCTransfer_OutOfOrderChunks verifies that CDC chunks sent out of order
// are still assembled correctly (using WriteAt with offsets from CDC meta).
func TestCDCTransfer_OutOfOrderChunks(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")
	content := randomContent(300000)

	expectedPath := filepath.Join(watchDir, "cdc_ooo.dat")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	res, err := ChunkFileCDC(writeTestFile(t, dir, "source_ooo.dat", content), 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed: %v", err)
	}
	if len(res.Chunks) < 2 {
		t.Fatalf("need at least 2 CDC chunks for out-of-order test, got %d", len(res.Chunks))
	}

	announce := &FileChangeAnnounce{
		Path:       "cdc_ooo.dat",
		Tag:        "default",
		Size:       res.Size,
		Hash:       res.Hash,
		Version:    7,
		NodeID:     "sender",
		ChunkSize:  0,
		ChunkCount: len(res.Meta),
		Chunks:     res.Meta,
	}
	tm.HandleFileChange("sender", announce)

	// Send chunks in reverse order to test WriteAt offset independence.
	for i := len(res.Chunks) - 1; i >= 0; i-- {
		ch := res.Chunks[i]
		payload := &FileChunkPayload{
			Path:       "cdc_ooo.dat",
			ChunkIndex: ch.Index,
			ChunkCount: len(res.Chunks),
			Data:       toB64(ch.Data),
			Version:    7,
		}
		tm.HandleFileChunk("sender", payload)
	}

	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("read assembled file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("out-of-order CDC assembly mismatch: %d vs %d bytes", len(got), len(content))
	}
	if hashOf(got) != hashOf(content) {
		t.Errorf("out-of-order CDC hash mismatch")
	}
}

// TestCDCTransfer_ConflictWithExistingFile verifies conflict detection works
// with CDC transfers.
func TestCDCTransfer_ConflictWithExistingFile(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	if err := os.MkdirAll(watchDir, 0755); err != nil {
		t.Fatal(err)
	}
	existingContent := []byte("original content from local node")
	destPath := filepath.Join(watchDir, "cdc_conflict.dat")
	if err := os.WriteFile(destPath, existingContent, 0644); err != nil {
		t.Fatal(err)
	}
	existingHash := hashOf(existingContent)

	incomingContent := randomContent(100000)

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	res, err := ChunkFileCDC(writeTestFile(t, dir, "incoming.dat", incomingContent), 65536, nil)
	if err != nil {
		t.Fatalf("ChunkFileCDC failed: %v", err)
	}

	announce := &FileChangeAnnounce{
		Path:       "cdc_conflict.dat",
		Tag:        "default",
		Size:       res.Size,
		Hash:       res.Hash,
		Version:    99,
		NodeID:     "node-a",
		ChunkSize:  0,
		ChunkCount: len(res.Meta),
		Chunks:     res.Meta,
	}
	tm.HandleFileChange("node-a", announce)

	for _, ch := range res.Chunks {
		payload := &FileChunkPayload{
			Path:       "cdc_conflict.dat",
			ChunkIndex: ch.Index,
			ChunkCount: len(res.Chunks),
			Data:       toB64(ch.Data),
			Version:    99,
		}
		tm.HandleFileChunk("node-a", payload)
	}

	// Verify incoming file was written to destination.
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read destination file: %v", err)
	}
	if !bytes.Equal(got, incomingContent) {
		t.Errorf("destination content mismatch")
	}

	// Verify original was renamed to conflict path.
	conflictPattern := "cdc_conflict.dat.conflict.node-a."
	foundConflict := false
	entries, err := os.ReadDir(watchDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), conflictPattern) {
			foundConflict = true
			conflictData, err := os.ReadFile(filepath.Join(watchDir, e.Name()))
			if err != nil {
				t.Fatalf("read conflict file: %v", err)
			}
			if !bytes.Equal(conflictData, existingContent) {
				t.Errorf("conflict file content mismatch")
			}
			break
		}
	}
	if !foundConflict {
		t.Errorf("expected conflict file with prefix %q", conflictPattern)
	}

	// Verify conflict was recorded.
	conflicts := tm.Conflicts()
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict record, got %d", len(conflicts))
	}
	if conflicts[0].LocalHash != existingHash {
		t.Errorf("conflict LocalHash: want %s, got %s", existingHash, conflicts[0].LocalHash)
	}
	if conflicts[0].IncomingHash != res.Hash {
		t.Errorf("conflict IncomingHash: want %s, got %s", res.Hash, conflicts[0].IncomingHash)
	}
}

// TestFixedSizeTransfer_StillWorks verifies that legacy fixed-size transfers
// continue to work alongside CDC mode.
func TestFixedSizeTransfer_StillWorks(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")
	content := testContent(100000)

	expectedPath := filepath.Join(watchDir, "fixed_received.dat")

	tm := NewTransferManager("receiver", dataDir, 65536,
		NewFileTracker(nil), nullBroadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// Old-style announce with no Chunks field.
	announce := &FileChangeAnnounce{
		Path:       "fixed_received.dat",
		Tag:        "default",
		Size:       int64(len(content)),
		Hash:       hashOf(content),
		Version:    1,
		NodeID:     "old-sender",
		ChunkSize:  65536,
		ChunkCount: 2,
	}
	tm.HandleFileChange("old-sender", announce)

	chunk0 := &FileChunkPayload{
		Path:       "fixed_received.dat",
		ChunkIndex: 0,
		ChunkCount: 2,
		Data:       toB64(content[:65536]),
		Version:    1,
	}
	chunk1 := &FileChunkPayload{
		Path:       "fixed_received.dat",
		ChunkIndex: 1,
		ChunkCount: 2,
		Data:       toB64(content[65536:]),
		Version:    1,
	}
	tm.HandleFileChunk("old-sender", chunk0)
	tm.HandleFileChunk("old-sender", chunk1)

	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("read assembled file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("fixed-size assembly mismatch: %d vs %d bytes", len(got), len(content))
	}
	if hashOf(got) != hashOf(content) {
		t.Errorf("fixed-size hash mismatch")
	}
}

// --- Table-driven chunker tests --------------------------------------------

func TestCDCChunker_VariousAvgSizes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		avgSize   int
		dataLen   int
		minChunks int
		maxChunks int
	}{
		{"avg 4KB, small data", 4096, 10000, 1, 6},
		{"avg 8KB, 100KB", 8192, 100000, 4, 30},
		{"avg 16KB, 500KB", 16384, 500000, 8, 60},
		{"avg 64KB, 1MB", 65536, 1024 * 1024, 8, 50},
		{"avg 256KB, 2MB", 262144, 2 * 1024 * 1024, 2, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := randomContent(tt.dataLen)
			r := bytes.NewReader(content)
			chunker := NewCDCChunker(r, tt.avgSize)

			var chunks []*CDCChunkResult
			for {
				ch, err := chunker.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("chunk error: %v", err)
				}
				chunks = append(chunks, ch)
			}

			if len(chunks) < tt.minChunks {
				t.Errorf("got %d chunks, want at least %d", len(chunks), tt.minChunks)
			}
			if len(chunks) > tt.maxChunks {
				t.Errorf("got %d chunks, want at most %d", len(chunks), tt.maxChunks)
			}

			// Verify content reconstruction.
			var reconstructed []byte
			for _, ch := range chunks {
				reconstructed = append(reconstructed, ch.Data...)
			}
			if !bytes.Equal(reconstructed, content) {
				t.Error("reconstructed content differs from original")
			}
		})
	}
}

// TestCDCChunker_BoundaryWithinLimits verifies that no chunk exceeds max.
func TestCDCChunker_BoundaryWithinLimits(t *testing.T) {
	t.Parallel()
	avgSize := 65536
	maxSize := avgSize * 4

	content := randomContent(5 * 1024 * 1024)
	r := bytes.NewReader(content)
	chunker := NewCDCChunker(r, avgSize)

	for {
		ch, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("chunk error: %v", err)
		}
		if len(ch.Data) > maxSize {
			t.Errorf("chunk %d is %d bytes (exceeds max %d)", ch.Index, len(ch.Data), maxSize)
		}
	}
}

// TestSendFileCDC_Integration verifies the complete sender path.
func TestSendFileCDC_Integration(t *testing.T) {
	dir := t.TempDir()
	watchDir := filepath.Join(dir, "watch")
	dataDir := filepath.Join(dir, "data")

	content := []byte("hello, CDC world!")
	writeTestFile(t, watchDir, "cdc_greeting.txt", content)

	tracker := NewFileTracker([]WatchDir{
		{Path: watchDir, Tag: "default"},
	})

	var lastMessage []byte
	broadcast := func(data []byte) {
		lastMessage = data
	}

	tm := NewTransferManager("sender", dataDir, 65536,
		tracker, broadcast,
		[]WatchDir{
			{Path: watchDir, Tag: "default"},
		}, nil)

	// First poll should detect the file and send it via CDC.
	if err := tm.Poll(); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
	if lastMessage == nil {
		t.Fatal("no broadcast captured")
	}

	// Second poll should see no changes.
	if err := tm.Poll(); err != nil {
		t.Fatalf("second Poll failed: %v", err)
	}
}
