package concurrent_test

import (
	"context"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
)

// TestStage_TransformsItems verifies that Stage applies the transform function
// to each input item and sends results to the output channel.
func TestStage_TransformsItems(t *testing.T) {
	ctx := context.Background()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	// Stage: double each number, keep all.
	output := concurrent.Stage(ctx, input, func(_ context.Context, n int) (int, bool) {
		return n * 2, true
	})

	var results []int
	for v := range output {
		results = append(results, v)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	// Pipeline Stage preserves order (single goroutine).
	expected := []int{2, 4, 6, 8, 10}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestStage_FiltersItems verifies that Stage drops items when keep=false.
func TestStage_FiltersItems(t *testing.T) {
	ctx := context.Background()

	input := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		input <- i
	}
	close(input)

	// Stage: keep only even numbers.
	output := concurrent.Stage(ctx, input, func(_ context.Context, n int) (int, bool) {
		return n, n%2 == 0
	})

	var results []int
	for v := range output {
		results = append(results, v)
	}

	expected := []int{2, 4, 6}
	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestStage_ChainedPipeline verifies that multiple stages can be composed
// into a multi-step pipeline.
func TestStage_ChainedPipeline(t *testing.T) {
	ctx := context.Background()

	// Generate: 1, 2, 3, 4, 5
	input := concurrent.Generate(ctx, []int{1, 2, 3, 4, 5})

	// Stage 1: filter evens → 2, 4
	evens := concurrent.Stage(ctx, input, func(_ context.Context, n int) (int, bool) {
		return n, n%2 == 0
	})

	// Stage 2: multiply by 10 → 20, 40
	multiplied := concurrent.Stage(ctx, evens, func(_ context.Context, n int) (int, bool) {
		return n * 10, true
	})

	var results []int
	for v := range multiplied {
		results = append(results, v)
	}

	expected := []int{20, 40}
	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// TestGenerate_CreatesChannelFromSlice verifies that Generate sends all
// items and closes the channel.
func TestGenerate_CreatesChannelFromSlice(t *testing.T) {
	ctx := context.Background()

	items := []string{"a", "b", "c"}
	ch := concurrent.Generate(ctx, items)

	var results []string
	for v := range ch {
		results = append(results, v)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
	for i, v := range results {
		if v != items[i] {
			t.Errorf("results[%d] = %q, want %q", i, v, items[i])
		}
	}
}

// TestGenerate_EmptySlice verifies that Generate handles empty input.
func TestGenerate_EmptySlice(t *testing.T) {
	ctx := context.Background()

	ch := concurrent.Generate(ctx, []int{})

	count := 0
	for range ch {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 items, got %d", count)
	}
}
