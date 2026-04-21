package concurrent

import (
	"context"
	"sync"
)

// FanOut reads from a single input channel and broadcasts each item
// to ALL output channels. Every consumer receives every item.
//
// This is a BROADCAST pattern — different from worker pool fan-out
// where each item goes to exactly ONE worker.
//
//	┌─────────────┐
//	│    input     │
//	└──────┬──────┘
//	       │
//	  ┌────▼────┐
//	  │ FanOut  │  reads each item once, sends to ALL outputs
//	  └────┬────┘
//	  ┌────┼────────┐
//	  ▼    ▼        ▼
//	[ch1] [ch2]  [chN]   ← each consumer gets ALL items
//
// Use case: an event should be simultaneously persisted, logged, and counted.
//
// KEY CONCEPTS:
//
// 1. Channel ownership: FanOut CREATES and CLOSES all output channels.
//    The caller receives them as <-chan T (receive-only).
//
// 2. Blocking behavior: if any consumer is slow, it blocks the fan-out
//    for ALL consumers. That's why output channels are buffered —
//    the buffer absorbs temporary speed differences between consumers.
//
// 3. Context cancellation: if the context is cancelled, the fan-out
//    stops immediately instead of blocking on a slow consumer.
func FanOut[T any](ctx context.Context, input <-chan T, numConsumers int) []<-chan T {
	// Create internal (writable) and external (read-only) channel slices.
	// The caller only gets read-only channels — they can't accidentally
	// close them or send to them.
	outputs := make([]chan T, numConsumers)
	readOnly := make([]<-chan T, numConsumers)

	for i := range numConsumers {
		// Buffered channels: absorb speed differences between consumers.
		// Buffer of 16 is a reasonable default — large enough to handle
		// small bursts, small enough to avoid excessive memory usage.
		ch := make(chan T, 16)
		outputs[i] = ch
		readOnly[i] = ch // implicit conversion: chan T → <-chan T
	}

	go func() {
		// FanOut owns these channels — it closes them when done.
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		// Read each item from input and broadcast to ALL outputs.
		for item := range input {
			for _, ch := range outputs {
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return readOnly
}

// Merge combines multiple input channels into a single output channel.
// This is the fan-in pattern: many producers → one consumer.
//
//	┌─────┐  ┌─────┐  ┌─────┐
//	│ ch1 │  │ ch2 │  │ chN │
//	└──┬──┘  └──┬──┘  └──┬──┘
//	   │        │        │
//	   └────────┼────────┘
//	            ▼
//	   ┌──────────────┐
//	   │    merged     │  ← single output with items from ALL inputs
//	   └──────────────┘
//
// KEY CONCEPTS:
//
// 1. One goroutine per input channel reads and forwards to the merged channel.
//    This ensures that a slow input doesn't block other inputs.
//
// 2. sync.WaitGroup ensures the merged channel is closed only when ALL
//    input channels are closed and fully drained.
//
// 3. Channel ownership: Merge creates and owns the merged channel.
//    Input channels must be closed by their respective producers.
//
// 4. The consumer doesn't need to know how many producers exist —
//    it just reads from the single merged channel.
func Merge[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	// Buffer = number of inputs: allows each input goroutine to send
	// one item without blocking, even if the consumer is momentarily busy.
	merged := make(chan T, len(channels))

	var wg sync.WaitGroup
	wg.Add(len(channels))

	// Start one forwarding goroutine per input channel.
	for _, ch := range channels {
		go func(c <-chan T) {
			defer wg.Done()
			for item := range c {
				select {
				case merged <- item:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	// Close merged channel when all inputs are exhausted.
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}
