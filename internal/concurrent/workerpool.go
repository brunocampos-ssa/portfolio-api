package concurrent

import (
	"context"
	"sync"
)

// RunWorkerPool starts numWorkers goroutines that read tasks from the input channel,
// process each task using the given function, and send results to the returned channel.
//
// ┌──────────┐     ┌──────────┐     ┌──────────┐
// │ Worker 1 │     │ Worker 2 │     │ Worker N │
// └────┬─────┘     └────┬─────┘     └────┬─────┘
//      │                │                │
//      └────────────────┼────────────────┘
//                       ▼
//              ┌─────────────────┐
//              │  output channel │  ← fan-in: all workers write here
//              └─────────────────┘
//
// KEY CONCEPTS:
//
// 1. Bounded concurrency: exactly numWorkers goroutines run at a time.
//    This prevents overwhelming external services or databases with too
//    many simultaneous requests.
//
// 2. Channel direction:
//    - input is <-chan I (receive-only): the pool only reads from it
//    - The returned channel is <-chan O (receive-only): the caller only reads
//    - Internally, the pool writes to the output channel (chan<- O)
//    This enforces correct usage at compile time.
//
// 3. Channel ownership:
//    - The CALLER owns the input channel and must close it when done sending.
//    - The POOL owns the output channel and closes it when all workers finish.
//    - Rule: the producer (creator) of a channel should close it.
//    - NEVER close a channel from the consumer side.
//
// 4. sync.WaitGroup coordinates worker completion:
//    - wg.Add(numWorkers) before starting workers
//    - Each worker calls wg.Done() when it finishes (via defer)
//    - A separate goroutine waits for all workers, then closes output
//
// 5. Buffer strategy:
//    - Output channel is buffered with numWorkers capacity.
//    - This prevents workers from blocking when they produce results faster
//      than the consumer reads them. Without buffering, a slow consumer
//      would stall all workers.
//    - We don't over-buffer: numWorkers is a reasonable upper bound for
//      how many results can be "in flight" at once.
func RunWorkerPool[I any, O any](
	ctx context.Context,
	numWorkers int,
	input <-chan I,
	process func(context.Context, I) O,
) <-chan O {
	// Buffered output channel: prevents workers from blocking on send.
	// Capacity = numWorkers because at most numWorkers results can be
	// produced simultaneously.
	output := make(chan O, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Start workers — each is a goroutine reading from the shared input channel.
	// When multiple goroutines read from the same channel, Go guarantees that
	// each item is delivered to exactly ONE reader. This is the fan-out pattern:
	// work is distributed across workers automatically.
	for i := range numWorkers {
		go func(workerID int) {
			defer wg.Done()

			// range over input: the loop exits when input is closed.
			// This is why the caller MUST close the input channel —
			// otherwise workers would block forever waiting for more items.
			for item := range input {
				// Check context before processing — allows fast shutdown
				// when the parent context is cancelled.
				select {
				case <-ctx.Done():
					return
				default:
				}

				result := process(ctx, item)

				// Send result to output, but respect context cancellation.
				// Without this select, a cancelled context could leave
				// goroutines blocked on a full output channel (goroutine leak).
				select {
				case output <- result:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Closer goroutine: waits for ALL workers to finish, then closes output.
	// This guarantees that the consumer (reading from output) will eventually
	// see the channel close and can exit its own range loop.
	//
	// IMPORTANT: only the owner (creator) of a channel should close it.
	// The pool created the output channel, so the pool closes it.
	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}
