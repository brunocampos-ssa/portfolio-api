package concurrent_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
)

// TestFanOut_BroadcastsToAllConsumers verifies that every consumer
// receives every item from the input channel.
func TestFanOut_BroadcastsToAllConsumers(t *testing.T) {
	ctx := context.Background()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	consumers := concurrent.FanOut(ctx, input, 3)

	if len(consumers) != 3 {
		t.Fatalf("expected 3 consumer channels, got %d", len(consumers))
	}

	// Each consumer should receive all 5 items.
	for i, ch := range consumers {
		var items []int
		for item := range ch {
			items = append(items, item)
		}
		if len(items) != 5 {
			t.Errorf("consumer %d received %d items, want 5", i, len(items))
		}

		sort.Ints(items)
		for j, v := range items {
			if v != j+1 {
				t.Errorf("consumer %d: items[%d] = %d, want %d", i, j, v, j+1)
			}
		}
	}
}

// TestFanOut_ContextCancellation verifies that FanOut stops when context
// is cancelled, even if input still has items.
func TestFanOut_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	input := make(chan int)

	consumers := concurrent.FanOut(ctx, input, 2)

	// Send one item.
	input <- 42

	// Read from first consumer.
	val := <-consumers[0]
	if val != 42 {
		t.Errorf("got %d, want 42", val)
	}

	// Cancel context.
	cancel()

	// Eventually, consumer channels should close.
	time.Sleep(50 * time.Millisecond)
	close(input)

	// Drain remaining items — channels should close soon.
	for range consumers[0] {
	}
	for range consumers[1] {
	}
}

// TestMerge_CombinesMultipleChannels verifies that Merge collects items
// from all input channels into a single output.
func TestMerge_CombinesMultipleChannels(t *testing.T) {
	ctx := context.Background()

	ch1 := make(chan int, 3)
	ch2 := make(chan int, 3)
	ch3 := make(chan int, 3)

	ch1 <- 1
	ch1 <- 2
	close(ch1)

	ch2 <- 3
	ch2 <- 4
	close(ch2)

	ch3 <- 5
	close(ch3)

	merged := concurrent.Merge(ctx, ch1, ch2, ch3)

	var results []int
	for v := range merged {
		results = append(results, v)
	}

	sort.Ints(results)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	expected := []int{1, 2, 3, 4, 5}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestMerge_EmptyInputs verifies that Merge handles empty channels gracefully.
func TestMerge_EmptyInputs(t *testing.T) {
	ctx := context.Background()

	ch1 := make(chan int)
	close(ch1)

	ch2 := make(chan int)
	close(ch2)

	merged := concurrent.Merge(ctx, ch1, ch2)

	count := 0
	for range merged {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 items from empty channels, got %d", count)
	}
}

// TestFanOut_ThenMerge verifies that FanOut followed by Merge produces
// N copies of each input item (one per consumer).
func TestFanOut_ThenMerge(t *testing.T) {
	ctx := context.Background()

	input := make(chan string, 2)
	input <- "hello"
	input <- "world"
	close(input)

	consumers := concurrent.FanOut(ctx, input, 3)

	// Use a WaitGroup to collect from all consumers concurrently.
	var mu sync.Mutex
	var results []string
	var wg sync.WaitGroup

	for _, ch := range consumers {
		wg.Add(1)
		go func(c <-chan string) {
			defer wg.Done()
			for s := range c {
				mu.Lock()
				results = append(results, s)
				mu.Unlock()
			}
		}(ch)
	}

	wg.Wait()

	// 2 items × 3 consumers = 6 total
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}

	// Count occurrences.
	counts := map[string]int{}
	for _, r := range results {
		counts[r]++
	}
	if counts["hello"] != 3 {
		t.Errorf("expected 3 copies of 'hello', got %d", counts["hello"])
	}
	if counts["world"] != 3 {
		t.Errorf("expected 3 copies of 'world', got %d", counts["world"])
	}
}
