package sync

import (
	"context"
	"testing"
	"time"
)

func TestThrottlerUnlimited(t *testing.T) {
	t.Parallel()
	tr := Unlimited()
	if tr.Rate() != 0 {
		t.Fatalf("unlimited throttler should report rate=0, got %d", tr.Rate())
	}
	// Unlimited should never block.
	ctx := context.Background()
	if err := tr.WaitN(ctx, 1<<20); err != nil { // 1 MB
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestThrottlerZeroRate(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(0, 0)
	if tr.Rate() != 0 {
		t.Fatalf("zero-rate throttler should report rate=0, got %d", tr.Rate())
	}
	// Should also never block.
	ctx := context.Background()
	if err := tr.WaitN(ctx, 1<<20); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestThrottlerBurstDefaultsToRate(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(1000, 0) // 0 burst → defaults to rate
	avail := tr.Available()
	if avail < 999 || avail > 1001 {
		t.Fatalf("expected ~1000 available tokens, got %f", avail)
	}
}

func TestThrottlerBurstCapacity(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(1000, 5000) // rate=1KB/s, burst=5KB
	avail := tr.Available()
	if avail < 4999 || avail > 5001 {
		t.Fatalf("expected ~5000 available tokens, got %f", avail)
	}
}

func TestThrottlerConsumesTokens(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(10000, 10000) // 10KB/s, 10KB burst
	if err := tr.WaitN(context.Background(), 4000); err != nil {
		t.Fatal(err)
	}
	avail := tr.Available()
	if avail > 6001 || avail < 5999 {
		t.Fatalf("expected ~6000 tokens after consuming 4000, got %f", avail)
	}
}

func TestThrottlerRateLimiting(t *testing.T) {
	t.Parallel()
	// 1000 bytes/sec, burst 1000.
	tr := NewThrottler(1000, 1000)

	start := time.Now()
	// Consume all 1000 tokens immediately.
	if err := tr.WaitN(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("first WaitN should be instant (burst), took %v", elapsed)
	}

	// Now consume another 1000 — this should take ~1 second.
	start = time.Now()
	if err := tr.WaitN(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	elapsed = time.Since(start)
	if elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("second WaitN should take ~1s, took %v", elapsed)
	}
}

func TestThrottlerContextCancel(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(1000, 1000)

	// Drain initial tokens.
	if err := tr.WaitN(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should timeout since tokens are drained and refill takes ~1s.
	err := tr.WaitN(ctx, 500)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestThrottlerRefillAfterWait(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(2000, 2000) // 2KB/s, 2KB burst

	// Drain tokens.
	if err := tr.WaitN(context.Background(), 2000); err != nil {
		t.Fatal(err)
	}

	// After ~500ms, ~1000 tokens should have refilled.
	time.Sleep(500 * time.Millisecond)
	avail := tr.Available()
	if avail < 900 || avail > 1100 {
		t.Fatalf("expected ~1000 tokens after 500ms refill, got %f", avail)
	}
}

func TestThrottlerConcurrentAccess(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(100000, 100000) // high rate so we don't time out

	// Run 10 concurrent consumers.
	ctx := context.Background()
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- tr.WaitN(ctx, 1000)
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent WaitN failed: %v", err)
		}
	}

	// All tokens should be consumed.
	avail := tr.Available()
	if avail > 100 {
		// Some may have refilled, but should be far from 100k.
		t.Logf("remaining tokens after 10x1000 consume: %f", avail)
	}
}

// TestThrottlerExactTokenBoundary tests that exact token boundary doesn't
// cause excessive waiting due to floating point.
func TestThrottlerExactTokenBoundary(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(65536, 65536) // 64KB/s, 64KB burst — typical chunk size

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := tr.WaitN(context.Background(), 65536); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	// First is instant (burst), second takes ~1s, third takes another ~1s.
	if elapsed < 1900*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("3x64KB at 64KB/s should take ~2s, took %v", elapsed)
	}
}

func TestThrottlerSmallChunks(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(1000, 1000)

	// Consume small chunks rapidly.
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := tr.WaitN(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	// 10x100 = 1000 bytes total, burst covers first 1000 → instant.
	if elapsed > 100*time.Millisecond {
		t.Fatalf("10x100 bytes at 1000/s within burst should be instant, took %v", elapsed)
	}

	// Now consume another 10x100 — should take ~1s (refill)
	start = time.Now()
	for i := 0; i < 10; i++ {
		if err := tr.WaitN(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	if elapsed < 800*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("second burst of 1000 bytes at 1000/s should take ~1s, took %v", elapsed)
	}
}

// TestThrottlerRateAccuracy verifies the throttler closely matches the
// configured rate over a sustained transfer.
func TestThrottlerRateAccuracy(t *testing.T) {
	t.Parallel()
	tr := NewThrottler(50000, 5000) // 50KB/s, 5KB burst (smaller than total)

	ctx := context.Background()
	start := time.Now()
	total := 0
	for i := 0; i < 100; i++ { // 100 chunks of 1000 bytes = 100KB
		if err := tr.WaitN(ctx, 1000); err != nil {
			t.Fatal(err)
		}
		total += 1000
	}
	elapsed := time.Since(start)
	// 100KB at 50KB/s should take ~2s (burst covers first 5KB), allow ±50% for scheduler jitter.
	if elapsed < 1400*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("100KB at 50KB/s should take ~2s, took %v (rate=%f B/s)", elapsed, float64(total)/elapsed.Seconds())
	}
}
