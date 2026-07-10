// Package sync — content-defined chunking via FastCDC (Gear hash).
package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"math/rand"
	"os"
)

// --- Gear table -------------------------------------------------------------

// gearTable is a pre-computed array of 256 random uint64 values used by the
// FastCDC rolling-hash algorithm. Generated at init with a fixed seed so the
// table is deterministic across runs and platforms.
var gearTable [256]uint64

func init() {
	rng := rand.New(rand.NewSource(42))
	for i := range gearTable {
		gearTable[i] = rng.Uint64()
	}
}

// --- Types ------------------------------------------------------------------

// CDCChunkMeta describes one content-defined chunk for the wire protocol.
// JSON tags match the existing convention in transfer.go.
type CDCChunkMeta struct {
	Index  int    `json:"index"`
	Offset int64  `json:"offset"`
	Size   int    `json:"size"`
	Hash   string `json:"hash"` // SHA-256 hex of this chunk's data
}

// CDCChunkResult is a single chunk produced by the CDC chunker, with its data.
type CDCChunkResult struct {
	Index  int
	Offset int64
	Data   []byte
	Hash   string
}

// CDCResult holds all data from CDC-chunking a file.
type CDCResult struct {
	Path   string
	Size   int64
	Hash   string // full-file SHA-256 hex
	Chunks []CDCChunkResult
	Meta   []CDCChunkMeta // wire-format chunk index
}

// --- CDCChunker -------------------------------------------------------------

// CDCChunker implements FastCDC content-defined chunking using the Gear hash
// rolling algorithm with normalised two-mask boundary detection.
//
// Algorithm reference:
//
//	Wen Xia et al., "The Design of Fast Content-Defined Chunking for Data
//	Deduplication Based Storage Systems" (USENIX ATC '16)
//
// Summary:
//   - A 256-entry gear table of random uint64 values is pre-computed.
//   - The rolling hash is updated as: hash = (hash << 1) + gear[byte].
//   - Chunk boundaries are declared when specific bits of the hash are zero.
//   - Two masks are used (maskS for the first region, maskL for the remainder)
//     to normalise chunk sizes around the target average.
//   - Minimum / average / maximum chunk sizes are enforced via clipping.
type CDCChunker struct {
	minSize  int
	maxSize  int
	normSize int // target average chunk size

	maskS uint64 // mask for [minSize, normSize) region
	maskL uint64 // mask for [normSize, maxSize) region

	rd     io.Reader
	buf    []byte
	cursor int
	offset int64
	eof    bool
	index  int

	hasher hash.Hash // accumulates the full-file SHA-256
}

// NewCDCChunker creates a CDC chunker that reads from rd and targets the
// given average chunk size (avgSize). The minimum chunk size is avgSize/4,
// and the maximum is avgSize*4. avgSize is clamped to at least 256 bytes.
func NewCDCChunker(rd io.Reader, avgSize int) *CDCChunker {
	if avgSize < 256 {
		avgSize = 256
	}
	minSize := avgSize / 4
	maxSize := avgSize * 4
	if minSize < 64 {
		minSize = 64
	}

	// Normalisation level 2 (standard FastCDC).
	bits := int(math.Round(math.Log2(float64(avgSize))))
	smallBits := bits + 2
	largeBits := bits - 2

	return &CDCChunker{
		minSize:  minSize,
		maxSize:  maxSize,
		normSize: avgSize,
		maskS:    (1 << smallBits) - 1,
		maskL:    (1 << largeBits) - 1,
		rd:       rd,
		buf:      make([]byte, maxSize*2),
		cursor:   maxSize * 2, // forces initial fillBuffer
		hasher:   sha256.New(),
	}
}

// Next returns the next content-defined chunk from the reader, or io.EOF
// when the reader is exhausted. The caller must not modify the returned Data
// slice after the next call to Next.
func (c *CDCChunker) Next() (*CDCChunkResult, error) {
	if err := c.fillBuffer(); err != nil {
		return nil, err
	}
	if len(c.buf) == 0 {
		return nil, io.EOF
	}

	data := c.buf[c.cursor:]
	length := c.findBoundary(data)

	chunkData := make([]byte, length)
	copy(chunkData, data[:length])

	// Per-chunk hash.
	ch := sha256.Sum256(chunkData)
	chunkHash := hex.EncodeToString(ch[:])

	// Accumulate into full-file hash.
	c.hasher.Write(chunkData)

	res := &CDCChunkResult{
		Index:  c.index,
		Offset: c.offset,
		Data:   chunkData,
		Hash:   chunkHash,
	}

	c.cursor += length
	c.offset += int64(length)
	c.index++

	return res, nil
}

// FullHash returns the hex-encoded SHA-256 of all data consumed so far.
// Must be called only after Next has returned io.EOF.
func (c *CDCChunker) FullHash() string {
	return hex.EncodeToString(c.hasher.Sum(nil))
}

// fillBuffer ensures at least maxSize bytes are available in the buffer.
func (c *CDCChunker) fillBuffer() error {
	n := len(c.buf) - c.cursor
	if n >= c.maxSize {
		return nil
	}
	// Slide remaining data to the front.
	copy(c.buf, c.buf[c.cursor:c.cursor+n])
	c.cursor = 0

	if c.eof {
		c.buf = c.buf[:n]
		return nil
	}

	m, err := io.ReadFull(c.rd, c.buf[n:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		c.buf = c.buf[:n+m]
		c.eof = true
	} else if err != nil {
		return err
	}
	return nil
}

// findBoundary scans data (which is at most maxSize bytes) and returns the
// chunk boundary position. It implements FastCDC's two-mask normalised search:
//
//	Phase 1 (minSize → normSize): use maskS (dense mask → higher boundary prob).
//	Phase 2 (normSize → maxSize): use maskL (sparse mask → lower boundary prob).
//	Fallback: return len(data) (clipped to maxSize by the caller).
func (c *CDCChunker) findBoundary(data []byte) int {
	if len(data) <= c.minSize {
		return len(data)
	}

	n := len(data)
	if n > c.maxSize {
		n = c.maxSize
	}

	var hash uint64
	i := c.minSize

	// Phase 1 — small mask (more bits checked → higher probability).
	limit1 := n
	if limit1 > c.normSize {
		limit1 = c.normSize
	}
	for ; i < limit1; i++ {
		hash = (hash << 1) + gearTable[data[i]]
		if hash&c.maskS == 0 {
			return i + 1
		}
	}

	// Phase 2 — large mask (fewer bits checked → lower probability).
	for ; i < n; i++ {
		hash = (hash << 1) + gearTable[data[i]]
		if hash&c.maskL == 0 {
			return i + 1
		}
	}

	return n
}

// --- File-level chunker -----------------------------------------------------

// ChunkFileCDC reads a regular file from disk and returns its CDC chunks with
// full metadata. avgSize is the target average chunk size (see NewCDCChunker).
func ChunkFileCDC(path string, avgSize int) (*CDCResult, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file: %q", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	chunker := NewCDCChunker(f, avgSize)
	var chunks []CDCChunkResult
	var metas []CDCChunkMeta

	for {
		ch, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("chunk %q: %w", path, err)
		}

		metas = append(metas, CDCChunkMeta{
			Index:  ch.Index,
			Offset: ch.Offset,
			Size:   len(ch.Data),
			Hash:   ch.Hash,
		})
		chunks = append(chunks, *ch)
	}

	// Empty file: produce a single zero-length chunk.
	if len(chunks) == 0 {
		h := sha256.Sum256(nil)
		emptyHash := hex.EncodeToString(h[:])
		chunks = append(chunks, CDCChunkResult{
			Index: 0,
			Data:  []byte{},
			Hash:  emptyHash,
		})
		metas = append(metas, CDCChunkMeta{
			Index: 0,
			Size:  0,
			Hash:  emptyHash,
		})
	}

	return &CDCResult{
		Path:   path,
		Size:   info.Size(),
		Hash:   chunker.FullHash(),
		Chunks: chunks,
		Meta:   metas,
	}, nil
}
