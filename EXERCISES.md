# Exercises: Advanced Concurrency in Go

> [Leia em Português](EXERCISES.pt-BR.md)

These exercises extend the `portfolio-api` project with a focus on the
`event-watcher` and `snapshot-runner` binaries. Each exercise asks you to
modify or extend real files in the codebase. Work through them in order —
later exercises assume earlier ones are done.

Before you start, make sure the project builds and the existing tests pass:

```bash
go build ./...
go test ./...
```

---

## Contents

| #  | Title | Difficulty | Key concepts |
|----|-------|------------|--------------|
| 1  | [Add an alert consumer to the fan-out](#exercise-1-add-an-alert-consumer-to-the-fan-out) | Beginner | Fan-out broadcast, directional channels, `select` |
| 2  | [Add a `total_usd` column to the snapshot](#exercise-2-add-a-total_usd-column-to-the-snapshot) | Beginner | Data pipeline, aggregation, SQL migration |
| 3  | [Count events per token in the metrics worker](#exercise-3-count-events-per-token-in-the-metrics-worker) | Beginner | `select` over multiple channels, `map` as accumulator |
| 4  | [Rate limiter for the worker pool](#exercise-4-rate-limiter-for-the-worker-pool) | Intermediate | `time.Ticker` as a semaphore, bounded concurrency |
| 5  | [Exponential-backoff retry for the LogsFetcher](#exercise-5-exponential-backoff-retry-for-the-logsfetcher) | Intermediate | Decorator pattern, `time.After`, `select` with `ctx.Done()` |
| 6  | [Integration test for the watcher pipeline](#exercise-6-integration-test-for-the-watcher-pipeline) | Intermediate | Mocks, end-to-end pipeline, channel draining |
| 7  | [Deduplication stage in the watcher](#exercise-7-deduplication-stage-in-the-watcher) | Intermediate | `concurrent.Stage`, map as a stateful filter |
| 8  | [Dynamic worker pool based on queue depth](#exercise-8-dynamic-worker-pool-based-on-queue-depth) | Advanced | Dynamic goroutines, `len(ch)`/`cap(ch)`, `sync.WaitGroup` |
| 9  | [Replace polling with WebSockets](#exercise-9-replace-polling-with-websockets) | Advanced | `gorilla/websocket`, `eth_subscribe`, channel composition |
| 10 | [Per-wallet timeout in the snapshot runner](#exercise-10-per-wallet-timeout-in-the-snapshot-runner) | Advanced | `context.WithTimeout`, partial failure, `select` |
| 11 | [Benchmark different worker pool sizes](#exercise-11-benchmark-different-worker-pool-sizes) | Advanced | `testing.B`, parametrized benchmarks, throughput analysis |
| 12 | ["diff" mode for the snapshot runner (Challenge)](#exercise-12-diff-mode-for-the-snapshot-runner-challenge) | Advanced | Composed pipeline, snapshot comparison, fan-in |

---

## Exercise 1: Add an alert consumer to the fan-out

**Difficulty:** Beginner

### Concept

`FanOut` in `internal/concurrent/fanout.go` broadcasts every event to **all**
registered consumers. The watcher currently has 3 consumers: `persistWorker`,
`logWorker`, and `metricsWorker`. In this exercise you'll add a fourth
consumer that acts as a simple alerting system.

The key insight is that adding a consumer to the fan-out is trivial: bump
`numConsumers` in the call to `concurrent.FanOut` and drain the extra
channel. Each consumer sees **every** event independently of the others.

### Instructions

1. **`internal/watcher/watcher.go`** — In `Run`, change the `FanOut` call
   from 3 to 4 consumers:

   ```go
   consumers := concurrent.FanOut(ctx, normalized, 4)
   ```

2. **`internal/watcher/watcher.go`** — Add the new consumer:

   ```go
   go w.alertWorker(ctx, consumers[3])
   ```

3. **`internal/watcher/watcher.go`** — Implement the `alertWorker` method:

   ```go
   func (w *Watcher) alertWorker(_ context.Context, events <-chan *domain.WalletEvent) {
       const threshold = 1000.0 // USD threshold — tune as needed

       for event := range events {
           // Parse event.Amount and compare to threshold.
           // Log an alert when it exceeds.
       }
       log.Println("watcher: alert worker done")
   }
   ```

   The worker should parse `event.Amount` (a string, e.g. `"1500.250000"`)
   with `strconv.ParseFloat`, and log an alert when the amount exceeds the
   threshold.

### Hint

- Note that `alertWorker` does **not** close the channel — it is a consumer.
  The channel is created and closed by `FanOut`. That's the ownership rule:
  the producer that creates a channel is responsible for closing it.
- Use `strconv.ParseFloat(event.Amount, 64)` to convert the amount. Silently
  skip parse errors with `continue`.
- The alert can be a simple `log.Printf` with an `[ALERT]` prefix.

### Acceptance

- `go build ./...` compiles with no errors.
- Run the `event-watcher` with wallets seeded in the database. When a
  high-value Transfer is detected, the `[ALERT]` message appears in the log.
- Verify that the other 3 workers (persist, log, metrics) keep working
  normally — the new consumer must not affect them.
- Write a unit test that creates a channel, sends an event with
  `Amount: "2000.000000"`, and verifies that `alertWorker` does not deadlock
  (the channel closes after the send).

---

## Exercise 2: Add a `total_usd` column to the snapshot

**Difficulty:** Beginner

### Concept

The snapshot runner in `internal/snapshot/runner.go` already computes
`USDValue` for each `WalletSnapshotItem`, but does not store the aggregate
total in the `WalletSnapshot` record. In this exercise you'll add a
`TotalUSD` field to the domain type and a matching column in the database.

This reinforces how data flows through the pipeline: the worker pool's
results are aggregated in stage 4 (collection), and that is the natural
place to compute the total.

### Instructions

1. **`migrations/005_add_total_usd.sql`** — Create a new migration:

   ```sql
   ALTER TABLE wallet_snapshots ADD COLUMN total_usd DOUBLE PRECISION NOT NULL DEFAULT 0;
   ```

2. **`internal/domain/snapshot.go`** — Add the field to the struct:

   ```go
   type WalletSnapshot struct {
       // ... existing fields ...
       TotalUSD      float64              `json:"total_usd" db:"total_usd"`
   }
   ```

3. **`internal/snapshot/runner.go`** — In stage 4 (after the
   `for result := range resultCh` loop), compute the total by summing
   `USDValue` across every item:

   ```go
   var totalUSD float64
   for _, item := range allItems {
       totalUSD += item.USDValue
   }
   snapshot.TotalUSD = totalUSD
   ```

4. **`internal/repository/postgres/snapshot_repository.go`** — Update the
   SQL queries to include the new `total_usd` column on `INSERT` and on
   the status `UPDATE`.

### Hint

- The sum must happen **after** all worker-pool results have been collected
  — i.e. after `for result := range resultCh` returns. That is the point
  where the fan-in has already converged every result into a single
  goroutine.
- Remember that `resultCh` only closes once **every** worker is done (via
  the internal `sync.WaitGroup` in `RunWorkerPool`). So when the `range`
  loop exits, you are guaranteed that every result sits in `allItems`.

### Acceptance

- Run the migration: `psql $DATABASE_URL -f migrations/005_add_total_usd.sql`.
- Run `go run ./cmd/snapshot-runner` and verify in the JSON output that
  `total_usd` appears with the correct sum of `usd_value` over all items.
- Query the database:
  `SELECT id, total_usd, status FROM wallet_snapshots ORDER BY created_at DESC LIMIT 1;`
  and confirm the value was persisted.

---

## Exercise 3: Count events per token in the metrics worker

**Difficulty:** Beginner

### Concept

`metricsWorker` in `internal/watcher/watcher.go` already counts events by
`Direction` (incoming/outgoing) using a `select` that multiplexes between
the events channel and a `time.Ticker`. In this exercise you'll add a
per-`TokenSymbol` count, learning how to use a `map` as an accumulator
inside a `select` loop.

### Instructions

1. **`internal/watcher/watcher.go`** — In `metricsWorker`, add a map to
   count by token:

   ```go
   byToken := make(map[string]int)
   ```

2. Inside `case event, ok := <-events:`, after the direction switch,
   increment the token counter:

   ```go
   byToken[event.TokenSymbol]++
   ```

3. At the three points where metrics are logged (channel closed, ticker,
   ctx.Done), include the token map in the message:

   ```go
   log.Printf("watcher: [METRICS] incoming=%d outgoing=%d total=%d byToken=%v",
       incoming, outgoing, incoming+outgoing, byToken)
   ```

### Hint

- The `map[string]int` is safe here because only **one goroutine** (the
  metricsWorker) reads and writes to it. If multiple goroutines needed to
  touch the map, you'd need a `sync.Mutex` or `sync.Map`. This is an
  important concept: data confined to a single goroutine needs no
  synchronization.
- `%v` in `log.Printf` prints the map in the form `map[USDC:5 USDT:3]`,
  which is enough for basic observability.

### Acceptance

- `go build ./...` compiles cleanly.
- Run the event-watcher and wait for at least one tick (15 seconds). The
  metrics message must include `byToken=map[...]` with the symbols of the
  detected tokens.
- Verify the map shows `USDC`, `USDT`, and `UNKNOWN` according to the
  events received.

---

## Exercise 4: Rate limiter for the worker pool

**Difficulty:** Intermediate

### Concept

`RunWorkerPool` in `internal/concurrent/workerpool.go` caps concurrency by
the number of goroutines (`numWorkers`) but does not cap **requests per
second**. For example, with 5 workers, if each request takes 100ms you
make ~50 req/s. But if each request takes 10ms, you make ~500 req/s —
which can exceed an RPC provider's rate limit (Infura, Alchemy, …).

In this exercise you'll implement a rate limiter using `time.Ticker` as a
semaphore: before processing an item, the worker waits for a "tick",
guaranteeing a minimum interval between requests.

### Instructions

1. **`internal/concurrent/workerpool.go`** — Create a new function
   `RunWorkerPoolWithRateLimit`:

   ```go
   func RunWorkerPoolWithRateLimit[I any, O any](
       ctx context.Context,
       numWorkers int,
       ratePerSecond int,
       input <-chan I,
       process func(context.Context, I) O,
   ) <-chan O {
       output := make(chan O, numWorkers)
       ticker := time.NewTicker(time.Second / time.Duration(ratePerSecond))

       var wg sync.WaitGroup
       wg.Add(numWorkers)

       for i := range numWorkers {
           go func(workerID int) {
               defer wg.Done()
               for item := range input {
                   // Wait for a tick before processing.
                   select {
                   case <-ticker.C:
                   case <-ctx.Done():
                       return
                   }

                   select {
                   case <-ctx.Done():
                       return
                   default:
                   }

                   result := process(ctx, item)

                   select {
                   case output <- result:
                   case <-ctx.Done():
                       return
                   }
               }
           }(i)
       }

       go func() {
           wg.Wait()
           ticker.Stop()
           close(output)
       }()

       return output
   }
   ```

2. **`internal/snapshot/runner.go`** — Replace the `RunWorkerPool` call
   with `RunWorkerPoolWithRateLimit`, targeting 5 requests/second (or a
   value configurable through a new `Runner` parameter).

### Hint

- `time.Ticker` acts as a simplified "token bucket": each tick releases one
  token, and the workers race for them. Because every worker shares the
  same ticker, at most `ratePerSecond` requests start per second regardless
  of how many workers exist.
- Heads up: with `ratePerSecond=5` and `numWorkers=5`, each worker averages
  one request per second. If `numWorkers > ratePerSecond`, some workers
  will idle most of the time — that's expected and not a problem.
- Don't forget `ticker.Stop()` in the cleanup goroutine to avoid leaking
  the timer.

### Acceptance

- Write a test in `internal/concurrent/workerpool_test.go` that:
  - Creates a `RunWorkerPoolWithRateLimit` with `ratePerSecond=10` and 3
    workers.
  - Sends 10 items through the input channel.
  - Measures the total processing time and verifies it is **at least**
    900ms (10 items / 10 per second ≈ 1 second).
  - Verifies that all 10 results were received on the output channel.
- Run `go test ./internal/concurrent/ -run TestRunWorkerPoolWithRateLimit -v`.

---

## Exercise 5: Exponential-backoff retry for the LogsFetcher

**Difficulty:** Intermediate

### Concept

`EthereumLogsFetcher` in `internal/provider/blockchain/ethereum_logs.go`
makes a single RPC attempt. If that call fails due to a transient error
(flaky network, temporarily unreachable RPC), the watcher simply logs the
error and waits for the next tick — potentially losing events.

In this exercise you'll build a `RetryLogsFetcher` decorator that wraps
any `contracts.LogsFetcher` and adds retry with exponential backoff. This
uses the decorator pattern: the wrapper implements the same interface as
the inner, adding behavior.

### Instructions

1. **Create `internal/provider/blockchain/retry_logs.go`** with the struct:

   ```go
   type RetryLogsFetcher struct {
       inner      contracts.LogsFetcher
       maxRetries int
       baseDelay  time.Duration
   }

   func NewRetryLogsFetcher(inner contracts.LogsFetcher, maxRetries int, baseDelay time.Duration) *RetryLogsFetcher {
       if inner == nil {
           panic("blockchain.NewRetryLogsFetcher: inner must not be nil")
       }
       return &RetryLogsFetcher{inner: inner, maxRetries: maxRetries, baseDelay: baseDelay}
   }
   ```

2. Implement `FetchLogs` with retry:

   ```go
   func (f *RetryLogsFetcher) FetchLogs(ctx context.Context, addresses []string, fromBlock uint64) ([]json.RawMessage, uint64, error) {
       var lastErr error
       for attempt := range f.maxRetries + 1 {
           logs, block, err := f.inner.FetchLogs(ctx, addresses, fromBlock)
           if err == nil {
               return logs, block, nil
           }
           lastErr = err
           if attempt == f.maxRetries {
               break
           }

           delay := f.baseDelay * time.Duration(1<<uint(attempt))
           log.Printf("retry_logs: attempt %d/%d failed: %v — retrying in %s",
               attempt+1, f.maxRetries+1, err, delay)

           select {
           case <-time.After(delay):
           case <-ctx.Done():
               return nil, 0, fmt.Errorf("retry cancelled: %w", ctx.Err())
           }
       }
       return nil, 0, fmt.Errorf("all %d attempts failed: %w", f.maxRetries+1, lastErr)
   }
   ```

3. **`cmd/event-watcher/main.go`** — Wrap `logsFetcher` with the retry
   decorator:

   ```go
   logsFetcher := blockchain.NewEthereumLogsFetcher(cfg.EthRPCURL)
   retryFetcher := blockchain.NewRetryLogsFetcher(logsFetcher, 3, 1*time.Second)
   // Use retryFetcher instead of logsFetcher when building the Watcher.
   ```

### Hint

- `select` over `time.After` and `ctx.Done()` is essential: if the context
  is cancelled during backoff (e.g. SIGTERM), the retry stops immediately
  instead of waiting out the full delay.
- Exponential backoff works as: attempt 0 = 1s, attempt 1 = 2s, attempt 2 =
  4s. The formula is `baseDelay * 2^attempt`, implemented with a bit shift:
  `1 << uint(attempt)`.
- In production you'd add jitter (random variance) to avoid a thundering
  herd. That is out of scope here, but leave a comment mentioning it.

### Acceptance

- Write tests in `internal/provider/blockchain/retry_logs_test.go`:
  - `TestRetryLogsFetcher_SucceedsFirstAttempt`: the inner returns success
    — verify 1 call.
  - `TestRetryLogsFetcher_RetriesOnError`: the inner fails twice then
    succeeds. Verify 3 calls total. Use `baseDelay` of 1ms so the test is
    quick.
  - `TestRetryLogsFetcher_ExhaustsRetries`: the inner always fails. Verify
    the final error contains `"all 4 attempts failed"`.
  - `TestRetryLogsFetcher_RespectsContextCancellation`: cancel the context
    before the second retry. Verify the call returns immediately with
    `context.Canceled`.
- Use a mock that counts calls via a `calls int` field on the struct.

---

## Exercise 6: Integration test for the watcher pipeline

**Difficulty:** Intermediate

### Concept

The watcher has a 3-stage pipeline: poller → normalizer → fan-out →
consumers. Testing each stage in isolation matters, but an integration
test that drives the full flow guarantees that the channels are correctly
wired together and that `close` propagates through the entire pipeline.

In this exercise you'll assemble the pipeline with mocks and verify that a
raw log inserted at the start reaches the final consumer as a normalized
`WalletEvent`.

### Instructions

1. **Create `internal/watcher/watcher_integration_test.go`** with the test
   `TestWatcherPipeline_EndToEnd`.

2. Build a `mockLogsFetcher` that returns a fixed list of raw JSON logs.
   Use the `RawLog` shape:

   ```go
   rawLog := watcher.RawLog{
       TxHash:      "0xabc123",
       BlockNumber: "0xa",
       Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
       Topics: []string{
           "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
           "0x0000000000000000000000001111111111111111111111111111111111111111", // from
           "0x0000000000000000000000002222222222222222222222222222222222222222", // to (tracked)
       },
       Data:    "0x00000000000000000000000000000000000000000000000000000000000f4240", // 1000000 (1 USDC)
       Removed: false,
   }
   ```

3. Assemble the pipeline manually (without going through `Watcher.Run`):

   ```go
   ctx, cancel := context.WithCancel(context.Background())
   defer cancel()

   trackedAddresses := map[string]string{
       "0x2222222222222222222222222222222222222222": "wallet-1",
   }

   // Stage 1: generate raw logs on the channel.
   rawCh := concurrent.Generate(ctx, rawLogs)

   // Stage 2: normalize.
   normalized := concurrent.Stage(ctx, rawCh, func(_ context.Context, raw watcher.RawLog) (*domain.WalletEvent, bool) {
       event, err := watcher.NormalizeTransferLog(raw, trackedAddresses)
       if err != nil || event == nil {
           return nil, false
       }
       return event, true
   })

   // Stage 3: fan-out to 1 consumer (simplified for the test).
   consumers := concurrent.FanOut(ctx, normalized, 1)

   // Collect results.
   var received []*domain.WalletEvent
   for event := range consumers[0] {
       received = append(received, event)
   }
   ```

4. Verify the received event carries the correct fields: `WalletID ==
   "wallet-1"`, `Direction == "incoming"`, `TokenSymbol == "USDC"`,
   `TxHash == "0xabc123"`.

### Hint

- The key point of this test is proving **close propagation**: when
  `Generate` closes the input channel, `Stage` finishes the remaining
  items and closes its output, which then makes `FanOut` close its
  downstream channels. The consumer's `range` ends naturally.
- If the test hangs (deadlock), some channel is likely not being closed
  correctly. Add a `context.WithTimeout` as a safety net:
  ```go
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  ```
- You don't need an `EventRepository` mock because the test does not hit
  `persistWorker`.

### Acceptance

- `go test ./internal/watcher/ -run TestWatcherPipeline_EndToEnd -v` passes.
- The test completes in under 1 second (no polling involved).
- Add a second raw log with `Removed: true` and verify it is filtered out
  by the normalizer (it must not appear in `received`).

---

## Exercise 7: Deduplication stage in the watcher

**Difficulty:** Intermediate

### Concept

On blockchains it's possible to receive the same event more than once —
for example, when the poller queries blocks that overlap the previous
window, or during a chain reorganization. In this exercise you'll add a
pipeline stage that filters duplicate events based on `TxHash`.

`concurrent.Stage` supports filtering natively: when the transform function
returns `(_, false)`, the item is dropped. You'll use a `map[string]bool`
to track which `TxHash` values have already been seen.

### Instructions

1. **`internal/watcher/watcher.go`** — In `Run`, add a deduplication stage
   between the normalizer and the fan-out:

   ```go
   // Stage 2: Normalizer
   normalized := concurrent.Stage(ctx, rawLogs, func(_ context.Context, raw RawLog) (*domain.WalletEvent, bool) {
       // ... existing code ...
   })

   // Stage 2.5: Deduplication (NEW)
   seen := make(map[string]bool)
   deduplicated := concurrent.Stage(ctx, normalized, func(_ context.Context, event *domain.WalletEvent) (*domain.WalletEvent, bool) {
       if seen[event.TxHash] {
           log.Printf("watcher: duplicate tx=%s — skipping", truncate(event.TxHash, 10))
           return nil, false
       }
       seen[event.TxHash] = true
       return event, true
   })

   // Stage 3: Fan-Out (now reading from deduplicated)
   consumers := concurrent.FanOut(ctx, deduplicated, 3)
   ```

2. Update the ASCII diagram in the `Run` doc comment to include the new
   stage.

### Hint

- The `map[string]bool` is safe without a `sync.Mutex` because `Stage`
  runs the transform in a **single goroutine**. Look at the implementation
  of `Stage` in `internal/concurrent/pipeline.go`: there is exactly one
  `go func()` processing items sequentially. If there were multiple
  goroutines, you'd need synchronization.
- In production, the `seen` map would grow without bound. A bonus exercise
  would be to cap the map (e.g. keep the most recent 10,000 hashes) or use
  a TTL. For now, the plain map is enough.
- The closure captures `seen` by reference, so the map persists between
  calls to the transform function.

### Acceptance

- Write a test in `internal/watcher/watcher_test.go` (or in the
  integration test from Exercise 6) that sends 3 raw logs through the
  pipeline, two of which share the same `TxHash`. Verify that only 2
  events leave the deduplication stage.
- Verify the log contains `"duplicate tx=... — skipping"` for the
  duplicate event.
- `go test ./internal/watcher/ -v` passes.

---

## Exercise 8: Dynamic worker pool based on queue depth

**Difficulty:** Advanced

### Concept

`RunWorkerPool` uses a fixed worker count. That's simple, but it doesn't
adapt to variable load. In this exercise you'll implement a worker pool
that tunes the goroutine count dynamically:

- If the input channel is > 80% full, add a worker (scale up).
- If the input channel is < 20% full, remove a worker (scale down).
- Respect a minimum (1) and a configurable maximum.

This teaches how to manage goroutine lifecycles dynamically using a
per-worker `context.CancelFunc` and a separate monitor goroutine.

### Instructions

1. **Create `internal/concurrent/dynamic_pool.go`** with:

   ```go
   func RunDynamicWorkerPool[I any, O any](
       ctx context.Context,
       minWorkers, maxWorkers int,
       input chan I,  // NOTE: chan I (bidirectional) — we need len() and cap()
       process func(context.Context, I) O,
   ) <-chan O
   ```

2. Internal strategy:
   - Maintain a list of `cancelFunc` values, one per active worker.
   - A "monitor" goroutine inspects `len(input)` and `cap(input)` every
     500ms.
   - If `len(input) > cap(input)*80/100` and workers < maxWorkers: start
     a new worker with its own `context.WithCancel`.
   - If `len(input) < cap(input)*20/100` and workers > minWorkers: cancel
     the context of the most recently added worker.
   - Use `sync.WaitGroup` to wait for all workers before closing the
     output channel.

3. **Important**: each worker must respect **its own** cancellation
   context (for scale-down) and the parent context (for global shutdown).

### Hint

- The `input` parameter needs to be `chan I` (bidirectional) rather than
  `<-chan I` because `len()` and `cap()` work more cleanly on bidirectional
  channels. In practice they work on `<-chan` too, but the bidirectional
  type makes the intent clearer.
- To cancel a single worker without affecting the others, derive a per-
  worker context: `workerCtx, workerCancel := context.WithCancel(ctx)`.
  Calling `workerCancel()` stops just that worker.
- Beware of races when accessing the list of workers. Use a `sync.Mutex`
  to protect the slice of cancel functions.
- A worker whose context was cancelled must exit the `for item := range
  input` loop gracefully via `select` with `workerCtx.Done()`.

### Acceptance

- Write a test in `internal/concurrent/dynamic_pool_test.go` that:
  - Creates an input channel with buffer 100.
  - Sends 90 items at once (> 80% of the buffer).
  - Verifies that extra workers are created (log the current worker count).
  - After all items are processed, verifies that workers are removed.
  - Verifies all 90 results are received on the output channel.
- Use `process` functions with `time.Sleep(10 * time.Millisecond)` to
  simulate work.
- `go test ./internal/concurrent/ -run TestRunDynamicWorkerPool -v -race`
  passes (the `-race` flag exposes race conditions).

---

## Exercise 9: Replace polling with WebSockets

**Difficulty:** Advanced

### Concept

The current watcher uses HTTP polling (`eth_getLogs` every N seconds). That
has inherent latency: events are only detected on the next poll. An
alternative is WebSockets with `eth_subscribe("logs", ...)`, which pushes
events in real time as soon as the Ethereum node emits them.

In this exercise you'll build an alternative poller that uses WebSockets
but keeps the **same downstream pipeline** (normalizer → fan-out →
consumers). This demonstrates the power of channels as an abstraction: the
poller produces `RawLog` values on a channel, and the rest of the pipeline
has no idea (and doesn't care) whether the data arrived over HTTP or over
WebSockets.

### Instructions

1. Add the dependency:

   ```bash
   go get github.com/gorilla/websocket
   ```

2. **Create `internal/provider/blockchain/ethereum_ws.go`** with a struct
   `EthereumWSLogsFetcher` exposing a `Subscribe` function:

   ```go
   func (f *EthereumWSLogsFetcher) Subscribe(ctx context.Context, addresses []string) (<-chan RawLog, error)
   ```

   The function must:
   - Connect to the WebSocket endpoint with
     `websocket.DefaultDialer.DialContext`.
   - Send an `eth_subscribe` message with a logs filter for the addresses.
   - Start a goroutine that reads messages from the socket and forwards
     `RawLog` values into the channel.
   - Close the channel and the socket when the context is cancelled.

3. **`internal/watcher/watcher.go`** — Add an alternative method
   `startWSPoller` that returns `<-chan RawLog` (same type as
   `startPoller`). The rest of the pipeline (`Stage`, `FanOut`, consumers)
   stays identical.

4. Add a flag or environment variable `WATCHER_MODE=ws|poll` to choose
   between the two modes.

### Hint

- The `eth_subscribe` request for logs looks like:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "eth_subscribe",
    "params": ["logs", {"topics": ["0xddf252ad..."]}],
    "id": 1
  }
  ```
  The node responds with a `subscription_id`, and from then on pushes
  notifications shaped as
  `{"method": "eth_subscription", "params": {"subscription": "0x...", "result": {...}}}`.
- The architectural key point: both `startPoller` and `startWSPoller`
  return `<-chan RawLog`. The downstream pipeline is identical because it
  consumes from the channel without caring about the source. **Channels
  as interface** — that's the abstraction.
- In tests, use a mock WebSocket server with `httptest.NewServer` and
  `websocket.Upgrader`.
- Providers like Infura and Alchemy expose WebSockets at `wss://` URLs.

### Acceptance

- `go build ./...` compiles.
- With a real WebSocket endpoint (e.g.
  `wss://mainnet.infura.io/ws/v3/YOUR_KEY`), run `go run
  ./cmd/event-watcher` with `WATCHER_MODE=ws` and verify that events
  arrive in real time (without the polling delay).
- Verify that `poll` mode still works.
- Write a unit test with a mock WebSocket server that sends 3 events and
  verifies they all arrive on the channel returned by `Subscribe`.

---

## Exercise 10: Per-wallet timeout in the snapshot runner

**Difficulty:** Advanced

### Concept

Currently, `processWallet` in `internal/snapshot/runner.go` has no
per-wallet timeout — if an RPC call hangs, the worker's goroutine stays
blocked indefinitely (until the parent context's global timeout fires, if
any).

In this exercise you'll add a per-wallet timeout using
`context.WithTimeout`, allowing the pipeline to keep processing other
wallets even when a single one is slow. This illustrates the **partial
failure** pattern with goroutines: individual failures must not stop the
batch.

### Instructions

1. **`internal/snapshot/runner.go`** — Add a `walletTimeout` field to the
   `Runner` struct:

   ```go
   type Runner struct {
       // ... existing fields ...
       walletTimeout time.Duration
   }
   ```

2. In `NewRunner`, set a default:

   ```go
   if walletTimeout <= 0 {
       walletTimeout = 10 * time.Second
   }
   ```

3. In `Run`, wrap the function passed to `RunWorkerPool` with a
   timeout-bounded context:

   ```go
   resultCh := concurrent.RunWorkerPool(ctx, r.numWorkers, walletCh, func(ctx context.Context, wallet domain.Wallet) walletResult {
       walletCtx, cancel := context.WithTimeout(ctx, r.walletTimeout)
       defer cancel()
       return r.processWallet(walletCtx, wallet, snapshot.ID)
   })
   ```

4. In `processWallet`, check the context before each RPC call:

   ```go
   if ctx.Err() != nil {
       return walletResult{
           WalletID: wallet.ID,
           Items:    items, // return the partial items already collected
           Error:    fmt.Errorf("wallet timeout: %w", ctx.Err()),
       }
   }
   ```

### Hint

- `context.WithTimeout` creates a derived context that cancels
  automatically after the specified duration. When `walletCtx` expires,
  every HTTP call made with it returns immediately with
  `context.DeadlineExceeded`.
- `defer cancel()` is required. Even if the timeout fires on its own, you
  still have to call `cancel()` to release timer resources. `go vet` will
  warn if you forget.
- Notice the partial-failure shape: if `GetBalance` for ETH succeeded but
  `GetTokenBalance` for USDC exceeded the deadline, the result contains
  the ETH item (partial) plus the error. Stage 4 (aggregation) decides
  how to handle that — in the current design, partial items are included
  in the snapshot.
- Don't confuse the per-wallet timeout with the global timeout in
  `cmd/snapshot-runner/main.go`
  (`context.WithTimeout(context.Background(), 5*time.Minute)`). They are
  nested contexts: wallet timeout (10s) < global timeout (5min).

### Acceptance

- Write a test in `internal/snapshot/runner_test.go` that:
  - Uses a mock `BalanceProvider` that blocks on
    `time.Sleep(20 * time.Second)`.
  - Sets `walletTimeout = 100 * time.Millisecond`.
  - Verifies that `Run` completes in under 1 second (it must not wait the
    full 20s).
  - Verifies the result contains an error matching `"deadline exceeded"`.
  - Verifies the snapshot has status `"completed"` (or `"failed"` if no
    wallet returned data).
- `go test ./internal/snapshot/ -run TestRunnerWalletTimeout -v -timeout 10s`
  passes.

---

## Exercise 11: Benchmark different worker pool sizes

**Difficulty:** Advanced

### Concept

How many workers is ideal? The answer depends on the kind of work
(CPU-bound vs I/O-bound), the latency of external calls, and the
provider's rate limit. In this exercise you'll build parametrized
benchmarks that measure snapshot-runner throughput at different pool
sizes.

Go has native benchmark support via `testing.B`. Benchmarks are functions
whose name starts with `Benchmark` instead of `Test`; the framework runs
the body `b.N` times to get a stable measurement.

### Instructions

1. **Create `internal/snapshot/runner_bench_test.go`** with parametrized
   benchmarks:

   ```go
   func BenchmarkSnapshotRunner(b *testing.B) {
       workerCounts := []int{1, 2, 4, 8, 16}

       for _, numWorkers := range workerCounts {
           b.Run(fmt.Sprintf("workers-%d", numWorkers), func(b *testing.B) {
               // Setup: build mocks that simulate network latency
               // with time.Sleep(10 * time.Millisecond).
               // Create a runner with numWorkers.

               b.ResetTimer()
               for i := 0; i < b.N; i++ {
                   _, err := runner.Run(ctx)
                   if err != nil {
                       b.Fatal(err)
                   }
               }
           })
       }
   }
   ```

2. The mocks should:
   - `WalletRepository.FindByBlockchain`: return 20 fixed wallets.
   - `BalanceProvider.GetBalance`: `time.Sleep(10ms)` + return a fixed value.
   - `TokenBalanceProvider.GetTokenBalance`: `time.Sleep(10ms)` + return a
     fixed value.
   - `PriceProvider.GetPriceUSD`: return a fixed value with no delay.
   - `SnapshotRepository`: no-op operations (don't persist).

3. Run the benchmarks and analyze the results.

### Hint

- Run with
  `go test ./internal/snapshot/ -bench=BenchmarkSnapshotRunner -benchtime=5s -v`.
- The output looks like:
  ```
  BenchmarkSnapshotRunner/workers-1    N    xxxxx ns/op
  BenchmarkSnapshotRunner/workers-2    N    xxxxx ns/op
  BenchmarkSnapshotRunner/workers-4    N    xxxxx ns/op
  ...
  ```
- With 20 wallets and 10ms per RPC call (ETH + 2 tokens = 3 calls per
  wallet):
  - 1 worker: ~20 × 30ms = 600ms
  - 4 workers: ~5 × 30ms = 150ms
  - 20 workers: ~1 × 30ms = 30ms
  In practice there is scheduling overhead and channel contention, so
  real numbers will differ.
- Use `b.ResetTimer()` after setup to exclude initialization.
- Use `benchstat` to compare runs statistically:
  ```bash
  go install golang.org/x/perf/cmd/benchstat@latest
  go test -bench=. -count=5 > old.txt
  # ... make changes ...
  go test -bench=. -count=5 > new.txt
  benchstat old.txt new.txt
  ```

### Acceptance

- The benchmarks run without errors.
- The results show that more workers reduce the runtime (up to a point).
- Identify the point of diminishing returns: beyond how many workers does
  the gain become negligible?
- Document your findings in a comment at the top of the benchmark file.

---

## Exercise 12: "diff" mode for the snapshot runner (Challenge)

**Difficulty:** Advanced

### Concept

Today each snapshot run produces a complete, independent record. But for
tax reports, what matters are the **changes**: which wallets had balance
deltas between two snapshots? In this exercise you'll implement a "diff"
mode that compares the freshly generated snapshot against the previous
one and reports the differences.

This combines several concurrency concepts:
- Pipeline to generate the new snapshot (already existing).
- Query for the previous snapshot (I/O).
- In-memory comparison (CPU).
- Output the diffs on a channel for flexibility.

### Instructions

1. **`internal/contracts/contracts.go`** — Add methods to the
   `SnapshotRepository` interface:

   ```go
   type SnapshotRepository interface {
       // ... existing methods ...
       FindLatestCompleted(ctx context.Context) (*domain.WalletSnapshot, error)
       FindSnapshotItems(ctx context.Context, snapshotID string) ([]domain.WalletSnapshotItem, error)
   }
   ```

2. **`internal/domain/snapshot.go`** — Add a type representing a diff:

   ```go
   type SnapshotDiff struct {
       WalletID    string  `json:"wallet_id"`
       AssetSymbol string  `json:"asset_symbol"`
       OldAmount   float64 `json:"old_amount"`
       NewAmount   float64 `json:"new_amount"`
       Change      float64 `json:"change"`      // new - old
       ChangeUSD   float64 `json:"change_usd"`  // change * usd_price
   }
   ```

3. **`internal/snapshot/runner.go`** — Add a `RunWithDiff` method:

   ```go
   func (r *Runner) RunWithDiff(ctx context.Context) (*domain.WalletSnapshot, []domain.SnapshotDiff, error) {
       // 1. Fetch the previous snapshot (FindLatestCompleted).
       // 2. Run the normal pipeline (r.Run).
       // 3. Compare the new items against the previous ones.
       // 4. Return the new snapshot and the list of diffs.
   }
   ```

4. The comparison must:
   - Build a map `key → WalletSnapshotItem` for the previous snapshot,
     using `walletID + ":" + assetSymbol` as the key.
   - Iterate the new snapshot's items and compare against the map.
   - Report new items (present in new but not previous).
   - Report removed items (present in previous but not new).
   - Report changed items (different amounts).

5. **`cmd/snapshot-runner/main.go`** — Add a `--diff` flag that calls
   `RunWithDiff` and prints the diffs alongside the snapshot.

### Hint

- The comparison doesn't need to be concurrent — it operates on data
  already collected in memory. The point of the exercise is integrating
  diff logic with the existing concurrent pipeline.
- To detect removed items, after iterating the new items, check which
  keys from the previous map were never visited.
- Use `math.Abs(new - old) < 0.000001` to treat values as equal
  (floating-point comparison). Values with differences smaller than that
  epsilon should count as unchanged.
- `FindLatestCompleted` should return `nil, nil` if no previous snapshot
  exists (first run). In that case every item is "new" and there are no
  meaningful diffs — return an empty slice.

### Acceptance

- Run `go run ./cmd/snapshot-runner` twice. On the second run with
  `--diff`, verify:
  - If no balance changed, the diff list is empty.
  - Add a new wallet to the database between runs and verify that the
    new wallet's items appear as "new" in the diff.
- Write a unit test that builds two in-memory `WalletSnapshotItem` sets
  and verifies the comparison logic (no database required).
- `go test ./internal/snapshot/ -run TestSnapshotDiff -v` passes.

---

## Wrap-up

After completing every exercise you will have:

- **Fan-out broadcast** with 4 independent consumers (Exercise 1)
- **Worker-pool aggregation** with persistence (Exercise 2)
- **Multiplexed `select`** using a map as an accumulator (Exercise 3)
- **Rate limiting** integrated into the worker pool (Exercise 4)
- **Exponential-backoff retry** using the decorator pattern (Exercise 5)
- **Integration test** covering the end-to-end pipeline (Exercise 6)
- **Stateful filter stage** in the pipeline (Exercise 7)
- **Dynamic worker pool** with auto scale-up/down (Exercise 8)
- **WebSocket as an alternative to polling** with the same channel interface (Exercise 9)
- **Per-item timeout** with partial failure (Exercise 10)
- **Parametrized benchmarks** for data-driven decisions (Exercise 11)
- **Composed pipeline** with snapshot comparison (Exercise 12)

Run the full test suite at the end:

```bash
go test ./... -v -count=1 -race
```

The `-race` flag enables Go's race detector — it flags concurrent access
to shared memory that is not guarded by synchronization. If any test fails
under `-race`, you have a real concurrency bug to fix.

---

# Module 3 — Professional Testing

These exercises sit on top of Module 2: they **don't** ask you to rewrite
the watcher or the snapshot runner. Instead, you'll write tests, fixtures,
mocks, and benchmarks around the code that already exists.

Before starting, make sure the baseline is green:

```bash
make test-unit
make test-race
```

## Contents — Module 3

| #   | Title | Difficulty | Key concepts |
|-----|-------|------------|--------------|
| M3.1 | Table-driven subtests for the normalizer | Beginner | `t.Run`, table tests, `testify/require` |
| M3.2 | Mocking `BalanceProvider` with `testify/mock` | Beginner | `mock.On`, `mock.Anything`, `AssertExpectations` |
| M3.3 | Context-cancellation test for the `snapshot.Runner` | Intermediate | `context.WithCancel`, `time.AfterFunc`, `atomic.Int32` |
| M3.4 | Integration test with `anvil_setBalance` | Intermediate | `testcontainers-go`, chain mutation, DB asserts |
| M3.5 | Benchmark and profile the normalizer | Intermediate | `go test -bench`, `-cpuprofile`, `go tool pprof` |
| M3.6 | `go test -race` on a deliberately planted bug | Intermediate | Race detector, `sync/atomic`, reading the stack trace |
| M3.7 | Impersonate + simulated transfer on Anvil | Advanced | `anvil_impersonateAccount`, `eth_sendTransaction`, full integration |

---

## Exercise M3.1: Table-driven subtests for the normalizer

**Difficulty:** Beginner

### Goal

Add an extra table of cases in
`internal/watcher/normalizer_extra_test.go` covering at least **four new
scenarios** for `NormalizeTransferLog`.

### Concepts involved

- Table tests in Go (`[]struct { name ...; want ... }`).
- Subtests with `t.Run(tt.name, func(t *testing.T) {...})` — each case
  shows up as an individual test in the output, which makes debugging
  failures easier.
- `testify/require` for preconditions, `testify/assert` for independent
  checks.

### Suggested files

- `internal/watcher/normalizer_extra_test.go` (add cases to
  `TestNormalizeTransferLog_TableDriven`).

### Acceptance criteria

- At least 4 new cases added (e.g. `BlockNumber=""`, unknown `Address`,
  `Removed=true` + tracked, tracked as `from` **and** `to`).
- Every test passes with `go test ./internal/watcher/... -v -count=1`.
- Scenario names (the `name` field) are descriptive; no `case1` filler.

---

## Exercise M3.2: Mocking `BalanceProvider` with testify/mock

**Difficulty:** Beginner

### Goal

Use `internal/testutil/mocks` to test a happy path and an error path of
`snapshot.Runner` **without** spinning up containers.

### Concepts involved

- `testify/mock`: `On`, `Return`, `Run`, `AssertExpectations`, `Maybe`.
- The difference between hand-rolled stubs (as in `runner_test.go`) and
  mocks (as in `runner_extra_test.go`).

### Instructions

1. Create a new file `internal/snapshot/runner_my_test.go`.
2. Write two tests:
   - `TestRunner_Run_PriceProviderRetriesTwice` — the price provider mock
     returns an error on the first two calls and a valid price on the
     third. Verify the final call count with
     `priceProvider.AssertNumberOfCalls(t, "GetPriceUSD", 3)`. (Hint: the
     current implementation does not retry — you'll see the test fail
     with `AssertExpectations`. Document the current behaviour in a
     comment; you don't have to implement the retry.)
   - `TestRunner_Run_NoTokenProvider_OnlyNativeItems` — pass `nil` as
     `tokenProvider` and verify that no `CreateSnapshotItem` call with
     `AssetType = "erc20"` is made.

### Suggested files

- `internal/snapshot/runner_my_test.go`

### Acceptance criteria

- Both described scenarios are covered.
- The mocks use `mock.On` with the right matchers (`mock.Anything` when
  it doesn't matter; concrete values when it does).
- `go test ./internal/snapshot/... -v` passes (with the caveat from the
  first test if retry isn't implemented).

---

## Exercise M3.3: Context-cancellation test for the `snapshot.Runner`

**Difficulty:** Intermediate

### Goal

Write a test that proves `snapshot.Runner` **stops processing wallets**
as soon as the context is cancelled, instead of draining the queue.

### Concepts involved

- `context.WithCancel`, `time.AfterFunc`.
- `sync/atomic` to count calls without a race condition.
- Go's contract: **cancelling a context does not kill goroutines** — it
  signals. Only code that properly observes `ctx.Done()` respects the
  cancel.

### Instructions

1. Follow the pattern of `TestRunner_Run_ContextCancellation` in
   `runner_extra_test.go`. Build a scenario with 20 wallets and 2 workers.
2. Configure `BalanceProvider.GetBalance` to block for 100ms while
   observing `ctx.Done()`.
3. Use `time.AfterFunc(150*time.Millisecond, cancel)` so that cancel
   fires after a single batch of workers has finished.
4. Assert that `balanceCalls.Load() < 20`.

### Suggested files

- Add a variant to `runner_extra_test.go` (or a new file).

### Acceptance criteria

- The test passes under `-race`:
  `go test ./internal/snapshot/... -race -v`.
- The test fails if you comment out the internal
  `select { case <-ctx.Done(): return }` branches in the worker pool (try
  it!).

---

## Exercise M3.4: Integration test with `anvil_setBalance`

**Difficulty:** Intermediate

### Goal

Write a new **integration test** that:

1. Uses `ethutil.SetEthBalance` to set 0 ETH on a brand-new address.
2. Runs the full `snapshot.Runner`.
3. Verifies that the resulting DB row has `amount = 0` and
   `usd_value = 0`.

### Concepts involved

- `testcontainers-go` + the `integration` build tag.
- Helpers in `internal/testutil/ethutil`.
- Direct assertions against live Postgres via `env.DB.QueryRowContext`.

### Instructions

1. Create `test/integration/snapshot_zero_balance_test.go`.
2. Copy the format of `snapshot_integration_test.go`: build tag,
   package, imports.
3. Seed a user + a wallet with `seedUserAndWallet`, then call
   `ethutil.SetEthBalance(ctx, rpc, addr, "0x0")` to force zero.
4. Run the runner, then query:
   ```go
   var amount, usdValue float64
   err := env.DB.QueryRowContext(ctx, `
       SELECT amount, usd_value FROM wallet_snapshot_items
       WHERE wallet_id = $1 AND asset_type = 'native'`, "w-zero").Scan(&amount, &usdValue)
   ```
5. Assert both are zero.

### Acceptance criteria

- `make test-integration` picks up the new test and it passes.
- If you comment out the `SetEthBalance` line and use
  `ethutil.EthToWei(1)` instead, the test fails correctly.

---

## Exercise M3.5: Benchmark and profile the normalizer

**Difficulty:** Intermediate

### Goal

Identify the hot path of `NormalizeTransferLog` using `pprof` and propose
one optimisation (implementing it is optional).

### Concepts involved

- `go test -bench` + `-cpuprofile`, `-memprofile`.
- `go tool pprof`, especially `-top` and `-list`.
- Reading `flat%` vs `cum%`.

### Instructions

1. Run:
   ```bash
   go test ./internal/watcher -bench=BenchmarkNormalizeTransferLog_Hit \
       -cpuprofile=cpu.out -benchtime=3s -run=^$
   ```
2. Analyse:
   ```bash
   go tool pprof -top -nodecount=10 cpu.out
   go tool pprof -list NormalizeTransferLog cpu.out
   ```
3. In a comment at the top of `normalizer_extra_test.go`, write 3 bullet
   points answering:
   - Which function eats the most CPU inside the normalizer?
   - Which allocation is the most expensive?
   - An optimisation proposal (e.g. reuse `big.Int` via `sync.Pool`;
     cache `strings.ToLower`; etc.).

### Acceptance criteria

- The benchmark runs and produces `cpu.out`.
- The comment exists, is specific, and is grounded in the pprof output.

---

## Exercise M3.6: `go test -race` on a deliberately planted bug

**Difficulty:** Intermediate

### Goal

Plant a race-condition bug inside `watcher.metricsWorker` and prove Go's
race detector finds it.

### Concepts involved

- Go's race detector (`-race`).
- When `int` **is not** safe for concurrent access.
- `sync/atomic.Int64` as a fix.

### Instructions

1. In `internal/watcher/watcher.go`, inside `metricsWorker`, replace the
   `incoming` and `outgoing` locals with shared state outside the
   goroutine (create an exported `metrics` struct with plain `int64`
   counters — **no atomic**).
2. In a new test
   `internal/watcher/watcher_race_test.go`, spin up a second goroutine
   that reads the counters while the watcher increments them. Run:
   ```bash
   go test ./internal/watcher/... -race -run TestWatcherMetricsRace
   ```
3. Observe the `WARNING: DATA RACE` output. Paste the stack trace into a
   comment inside the test.
4. Fix it with `atomic.Int64` and prove `-race` comes back green.
5. **At the end, revert every production change** — the original code
   had no bug. The exercise is didactic only.

### Acceptance criteria

- Before the fix: `-race` reports a data race with a stack trace
  pointing at the shared field.
- After the fix: `-race` is clean.
- The exercise reverts the production changes and keeps only the test
  (adjusted to the original counters) on a separate branch.

---

## Exercise M3.7: Impersonate + simulated transfer on Anvil

**Difficulty:** Advanced

### Goal

Use `ethutil.Impersonate` + `eth_sendTransaction` to simulate an on-chain
transfer between two wallets, then validate that the watcher (running
against Anvil) persists the event in Postgres.

### Concepts involved

- `anvil_impersonateAccount` — acting as an address without its private
  key.
- `eth_sendTransaction` via `ethutil.Client.Call`.
- End-to-end integration: watcher + Postgres + Anvil.

### Instructions

1. In `test/integration/watcher_integration_test.go` (new file with
   `//go:build integration`):
   - Seed a tracked wallet (`w-source`) with 10 ETH.
   - Impersonate `w-source`.
   - Call `eth_sendTransaction` with `from=w-source`, `to=w-dest`,
     `value=0x1`.
   - Start `watcher.NewWatcher(...).Run(ctx)` in a goroutine.
   - Wait until the event appears in the `wallet_events` table (poll
     with a reasonable timeout, e.g. 10s).
2. **Heads up:** the current watcher only filters **ERC-20 Transfer**
   events, not native ETH transfers. You'll need to deploy a simple
   ERC-20 contract on Anvil (or use `anvil_setCode` to inject a mock)
   and call `transfer` on it. Alternatively, document that native ETH
   isn't captured by the current watcher and write the test against a
   pre-loaded ERC-20 contract.

### Acceptance criteria

- `make test-integration` passes.
- The test verifies in the DB that a `wallet_events` row with
  `direction=incoming` or `outgoing` was persisted.
- The test cleans up the chain / DB between runs (idempotent).

---

## General tips — Module 3

1. **Use `t.Context()`** instead of `context.Background()` in tests. It
   cancels automatically when the test finishes, which helps
   testcontainers clean up orphan containers.
2. **Avoid `time.Sleep`** for synchronising tests. Prefer channels,
   `sync.WaitGroup`, or testify's `require.Eventually(...)`.
3. **Benchmarks are not a ranking.** `1ms/op` on an M1 laptop can be
   `3ms/op` on x86 CI. Use benchmarks to **compare alternatives on the
   same machine**, not to report absolute numbers.
4. **The race detector is not magic.** It only catches races that
   actually happened during the test run. Combine it with property-style
   tests (counts, sums) that fail even when the race isn't observed.
