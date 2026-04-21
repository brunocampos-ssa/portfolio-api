package concurrent

import "context"

// Stage creates a pipeline stage that transforms items from an input channel
// and sends results to a new output channel.
//
//	┌─────────┐    ┌──────────────┐    ┌──────────┐
//	│  input   │ →  │  transform   │ →  │  output  │
//	└─────────┘    └──────────────┘    └──────────┘
//
// The transform function returns (result, keep):
//   - keep=true: the result is sent to the output channel
//   - keep=false: the item is filtered out (dropped)
//
// KEY CONCEPTS:
//
// 1. Pipeline pattern: data flows through sequential stages, each
//    performing a specific transformation. Stages are composable —
//    you can chain Stage(ctx, Stage(ctx, input, f1), f2) to build
//    multi-step pipelines.
//
// 2. Filtering: the bool return allows stages to drop items.
//    For example, a normalizer stage can drop malformed events.
//
// 3. Closing propagation: when input closes, the stage finishes
//    processing remaining items and closes its output.
//    This creates a "close cascade" through the entire pipeline:
//    source closes → stage 1 closes → stage 2 closes → consumer exits.
//
// 4. Context awareness: every stage checks ctx.Done() to support
//    graceful shutdown of the entire pipeline.
//
// 5. Buffer strategy: output is buffered with capacity 1.
//    This allows the producer goroutine to stay one step ahead of
//    the consumer, improving throughput without excessive memory usage.
//    For CPU-bound transforms, this small buffer is sufficient.
//    For I/O-bound transforms, consider using RunWorkerPool instead.
func Stage[I any, O any](
	ctx context.Context,
	input <-chan I,
	transform func(context.Context, I) (O, bool),
) <-chan O {
	// Buffer of 1: allows the stage to produce one result while the
	// consumer is processing the previous one. This is the simplest
	// way to decouple producer and consumer speeds.
	output := make(chan O, 1)

	go func() {
		// Stage owns the output channel — it closes when done.
		// This propagates the "close signal" to downstream stages.
		defer close(output)

		for item := range input {
			// Check for shutdown before processing.
			select {
			case <-ctx.Done():
				return
			default:
			}

			result, keep := transform(ctx, item)
			if !keep {
				continue // filter: skip this item
			}

			// Send result, respecting context cancellation.
			select {
			case output <- result:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

// Generate creates a channel and populates it with the given items.
// This is a convenience function to start a pipeline from a slice.
//
// Channel ownership: Generate creates the channel and closes it
// after all items are sent. The returned channel is receive-only.
//
// Buffer strategy: the channel is buffered with the slice length,
// so all items can be sent without blocking. This is appropriate
// when the input set is known and bounded (e.g., wallets from DB).
func Generate[T any](ctx context.Context, items []T) <-chan T {
	ch := make(chan T, len(items))

	go func() {
		defer close(ch)
		for _, item := range items {
			select {
			case ch <- item:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
