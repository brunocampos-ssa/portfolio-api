package concurrent_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
)

// =============================================================================
// Concurrency-focused tests — designed to run under `go test -race`.
//
// Each test here targets a specific kind of race-condition regression we'd
// want to catch if students (or future us) refactored the primitives.
// =============================================================================

// TestWorkerPool_NoGoroutineLeakOnCancellation checks that cancelling the
// context while work is in flight lets all goroutines return. If the pool
// ever added `chan<-` branches without `select { case <-ctx.Done(): }`, the
// goroutine count would keep climbing across runs.
func TestWorkerPool_NoGoroutineLeakOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	// A closed-on-demand input channel simulates an unbounded producer.
	input := make(chan int)
	go func() {
		defer close(input)
		i := 0
		for {
			select {
			case input <- i:
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	var active atomic.Int32
	output := concurrent.RunWorkerPool(ctx, 4, input, func(ctx context.Context, n int) int {
		active.Add(1)
		defer active.Add(-1)
		// Block until ctx is done so the cancellation path is exercised
		// for at least some of the workers.
		<-ctx.Done()
		return n
	})

	// Give workers a moment to start and grab items.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// After cancellation the output must eventually close.
	closed := make(chan struct{})
	go func() {
		for range output {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("output channel never closed — workers leaked")
	}
	require.Equal(t, int32(0), active.Load(), "all workers should have stopped")
}

// TestFanOut_BroadcastUnderConcurrentConsumers makes every consumer do real
// work concurrently. Run with `-race` to catch any shared-state slip inside
// FanOut.
func TestFanOut_BroadcastUnderConcurrentConsumers(t *testing.T) {
	const items = 200

	input := make(chan int, items)
	for i := 1; i <= items; i++ {
		input <- i
	}
	close(input)

	consumers := concurrent.FanOut(t.Context(), input, 5)

	var wg sync.WaitGroup
	var totals [5]atomic.Int64
	for i, ch := range consumers {
		wg.Add(1)
		go func(idx int, ch <-chan int) {
			defer wg.Done()
			for v := range ch {
				totals[idx].Add(int64(v))
			}
		}(i, ch)
	}
	wg.Wait()

	// Sum 1..items == items*(items+1)/2.
	expected := int64(items * (items + 1) / 2)
	for i := range totals {
		require.Equalf(t, expected, totals[i].Load(),
			"consumer %d should have seen every item", i)
	}
}

// TestMerge_FairCollectionUnderLoad streams data from several producers
// concurrently and verifies Merge loses no items.
func TestMerge_FairCollectionUnderLoad(t *testing.T) {
	const producers = 6
	const perProducer = 500

	channels := make([]<-chan int, producers)
	for i := range producers {
		ch := make(chan int, perProducer)
		channels[i] = ch
		go func(ch chan<- int, base int) {
			defer close(ch)
			for j := range perProducer {
				ch <- base*perProducer + j
			}
		}(ch, i)
	}

	merged := concurrent.Merge(t.Context(), channels...)

	got := 0
	for range merged {
		got++
	}
	require.Equal(t, producers*perProducer, got)
}

// TestStage_ContextCancellation_StopsPropagation verifies that when the
// context is cancelled mid-flight, the downstream channel closes and no
// transform call leaks past cancellation.
func TestStage_ContextCancellation_StopsPropagation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	input := make(chan int)
	go func() {
		defer close(input)
		for i := range 1000 {
			select {
			case input <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	var processed atomic.Int64
	out := concurrent.Stage(ctx, input, func(_ context.Context, n int) (int, bool) {
		processed.Add(1)
		time.Sleep(5 * time.Millisecond)
		return n, true
	})

	// Let a few items through, then cancel.
	time.Sleep(30 * time.Millisecond)
	cancel()

	// After cancellation the output channel must close.
	closedOK := make(chan struct{})
	go func() {
		for range out {
		}
		close(closedOK)
	}()
	select {
	case <-closedOK:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stage did not close output after context cancellation")
	}

	require.Less(t, int(processed.Load()), 1000,
		"cancellation should truncate the pipeline")
}
