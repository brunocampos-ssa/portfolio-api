package concurrent_test

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
)

// TestRunWorkerPool_ProcessesAllItems verifies that every input item
// is processed exactly once and all results are collected.
func TestRunWorkerPool_ProcessesAllItems(t *testing.T) {
	ctx := context.Background()

	// Create input channel with 10 items.
	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input) // Producer must close input — pool waits for this.

	// Process: double each number.
	output := concurrent.RunWorkerPool(ctx, 3, input, func(_ context.Context, n int) int {
		return n * 2
	})

	// Collect results.
	var results []int
	for r := range output {
		results = append(results, r)
	}

	// Verify all items were processed.
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	// Sort because worker pool doesn't guarantee order.
	sort.Ints(results)
	expected := []int{2, 4, 6, 8, 10, 12, 14, 16, 18, 20}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestRunWorkerPool_BoundedConcurrency verifies that at most numWorkers
// goroutines are processing simultaneously.
func TestRunWorkerPool_BoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	numWorkers := 3

	input := make(chan int, 20)
	for i := 0; i < 20; i++ {
		input <- i
	}
	close(input)

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	output := concurrent.RunWorkerPool(ctx, numWorkers, input, func(_ context.Context, n int) int {
		cur := currentConcurrent.Add(1)
		// Track the maximum concurrency observed.
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}

		time.Sleep(10 * time.Millisecond) // simulate work
		currentConcurrent.Add(-1)
		return n
	})

	// Drain output.
	for range output {
	}

	max := int(maxConcurrent.Load())
	if max > numWorkers {
		t.Errorf("max concurrent workers = %d, want <= %d", max, numWorkers)
	}
	if max == 0 {
		t.Error("no workers ran")
	}
}

// TestRunWorkerPool_ContextCancellation verifies that workers stop
// when the context is cancelled.
func TestRunWorkerPool_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	input := make(chan int, 100)
	for i := 0; i < 100; i++ {
		input <- i
	}
	close(input)

	var processed atomic.Int32

	output := concurrent.RunWorkerPool(ctx, 2, input, func(ctx context.Context, n int) int {
		processed.Add(1)
		time.Sleep(50 * time.Millisecond) // slow processing
		return n
	})

	// Cancel after a short delay — not all items should be processed.
	time.Sleep(80 * time.Millisecond)
	cancel()

	// Drain remaining output.
	for range output {
	}

	p := int(processed.Load())
	if p >= 100 {
		t.Errorf("expected fewer than 100 processed items after cancellation, got %d", p)
	}
}

// TestRunWorkerPool_EmptyInput verifies that the pool handles an empty
// input channel gracefully — output should close immediately.
func TestRunWorkerPool_EmptyInput(t *testing.T) {
	ctx := context.Background()

	input := make(chan int)
	close(input) // empty input

	output := concurrent.RunWorkerPool(ctx, 3, input, func(_ context.Context, n int) int {
		t.Error("process should never be called for empty input")
		return n
	})

	// Output should close immediately.
	count := 0
	for range output {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 results for empty input, got %d", count)
	}
}
