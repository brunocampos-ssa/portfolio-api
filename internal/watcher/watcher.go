package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Watcher — Ethereum Event Monitor
// =============================================================================
//
// The Watcher monitors Ethereum for ERC-20 Transfer events involving wallets
// that are tracked in the database.
//
// Architecture (Class 2 — Kafka as event source-of-truth):
//
//   ┌──────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌────────────────┐
//   │  eth_getLogs  │ ──► │  rawLogs    │ ──► │  normalizer      │ ──► │ kafka publisher │
//   │  (poller)     │     │  chan RawLog │     │  (pipeline stage)│     │ (publish stage) │
//   └──────────────┘     └─────────────┘     └──────────────────┘     └────────┬───────┘
//                                                                                 │
//                                                                          Kafka topic
//                                                                                 │
//                                                          ┌──────────────────────┼──────────────────────┐
//                                                          ▼                      ▼                      ▼
//                                                ┌──────────────────┐    ┌──────────────────┐   ┌──────────────────┐
//                                                │ event-persister  │    │  event-router    │   │ event-analytics  │
//                                                │ (CG: persister)  │    │ (CG: router)     │   │ (CG: analytics)  │
//                                                └──────────────────┘    └──────────────────┘   └──────────────────┘
//
// Class 1 had three in-process consumers (persist/log/metrics) hanging
// off a `concurrent.FanOut`. Class 2 collapses that into a single Kafka
// publish stage and moves the consumers into separate processes (one
// Kafka consumer group each — see broker/topics.go and cmd/event-*).
// Two wins: the watcher process no longer owns persistence, and we
// gain durability + replay because the events live in a partitioned
// log instead of evaporating with the process.
//
// CONCURRENCY CONCEPTS still on display:
//
//  1. Directional channels (<-chan, chan<-) on every stage boundary so
//     direction is enforced at compile time.
//  2. Channel ownership: each stage owns + closes its output. Close
//     cascades from the poller all the way through to the publish loop.
//  3. Advanced select in the poller — multiplexing ticker, heartbeat,
//     and ctx.Done() with starvation-free pseudo-random fairness.
//  4. Backpressure all the way to the wire: the publisher's
//     synchronous WriteMessages slows the publish loop, which fills
//     the normalize Stage's output channel, which slows the poller —
//     a Kafka outage throttles polling instead of leaking memory.
//  5. Graceful shutdown via context cancellation propagating through
//     every stage.

// Watcher is the main event monitoring service.
type Watcher struct {
	logsFetcher  contracts.LogsFetcher
	walletRepo   contracts.WalletRepository
	publisher    broker.Publisher
	pollInterval time.Duration
}

// NewWatcher creates a new event watcher.
//
// Dependencies:
//   - logsFetcher: polls Ethereum for event logs (eth_getLogs)
//   - walletRepo: loads tracked wallet addresses from the database
//   - publisher: emits normalized events onto the broker bus (Kafka in
//     production, an in-memory mock in unit tests). The watcher does
//     NOT persist events directly — that responsibility moved to
//     event-persister, a separate consumer of the same Kafka topic.
//   - pollInterval: how often to poll for new logs
func NewWatcher(
	logsFetcher contracts.LogsFetcher,
	walletRepo contracts.WalletRepository,
	publisher broker.Publisher,
	pollInterval time.Duration,
) *Watcher {
	if logsFetcher == nil {
		panic("watcher.NewWatcher: logsFetcher must not be nil")
	}
	if walletRepo == nil {
		panic("watcher.NewWatcher: walletRepo must not be nil")
	}
	if publisher == nil {
		panic("watcher.NewWatcher: publisher must not be nil")
	}
	return &Watcher{
		logsFetcher:  logsFetcher,
		walletRepo:   walletRepo,
		publisher:    publisher,
		pollInterval: pollInterval,
	}
}

// Run starts the watcher. It blocks until the context is cancelled.
//
// Pipeline:
//
//   1. Poller produces raw logs → rawLogs channel
//   2. Stage normalizes them    → normalized channel
//   3. Publish stage marshals + writes each event to Kafka. Synchronous —
//      a slow broker propagates backpressure all the way to the poller.
func (w *Watcher) Run(ctx context.Context) error {
	log.Println("watcher: loading tracked wallets...")

	trackedAddresses, err := w.loadTrackedAddresses(ctx)
	if err != nil {
		return fmt.Errorf("watcher: load tracked addresses: %w", err)
	}

	if len(trackedAddresses) == 0 {
		log.Println("watcher: no ethereum wallets to watch, waiting for shutdown...")
		<-ctx.Done()
		return ctx.Err()
	}

	log.Printf("watcher: tracking %d ethereum addresses", len(trackedAddresses))

	addresses := make([]string, 0, len(trackedAddresses))
	for addr := range trackedAddresses {
		addresses = append(addresses, addr)
	}

	// =========================================================================
	// STAGE 1: Poller
	//
	// startPoller owns rawLogs — creates it, sends into it, and closes it
	// on shutdown. Returns <-chan so downstream code cannot accidentally
	// close it.
	// =========================================================================
	rawLogs := w.startPoller(ctx, addresses)

	// =========================================================================
	// STAGE 2: Normalizer
	//
	// Filters non-Transfer logs and resolves the address → wallet_id /
	// direction mapping. Skipped events are dropped silently. The
	// concurrent.Stage helper handles close propagation: when rawLogs
	// closes, the normalizer drains it and closes its output too.
	// =========================================================================
	normalized := concurrent.Stage(ctx, rawLogs, func(_ context.Context, raw RawLog) (*domain.WalletEvent, bool) {
		event, err := NormalizeTransferLog(raw, trackedAddresses)
		if err != nil {
			log.Printf("watcher: normalize error: %v", err)
			return nil, false
		}
		if event == nil {
			return nil, false
		}
		return event, true
	})

	// =========================================================================
	// STAGE 3: Publish to Kafka
	//
	// Replaces Class 1's FanOut + persist/log/metrics workers. A single
	// goroutine drains normalized and Publishes each event onto the
	// broker bus. The publisher's WriteMessages call is synchronous, so
	// when Kafka is slow the publish goroutine slows, which fills the
	// normalize stage's output channel, which slows the poller. The
	// process never accumulates an unbounded backlog of unwritten events.
	//
	// Why one goroutine and not a pool? kafka-go's Writer batches
	// internally; multiple goroutines competing for the same Writer
	// don't increase throughput. If we ever need parallel publish, the
	// move is to shard the input channel by partition key, not to
	// fan out to N goroutines on the same channel.
	// =========================================================================
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		w.runPublishStage(ctx, normalized)
	}()

	log.Println("watcher: running — press Ctrl+C to stop")

	<-ctx.Done()
	log.Println("watcher: shutting down gracefully...")

	// Wait for the publish goroutine to drain and exit before returning.
	// Without this the test (and `make watcher`) can race past in-flight
	// Publish calls, losing events that were already normalized.
	<-publishDone
	return ctx.Err()
}

// publishBackoffInitial / publishBackoffMax cap the per-event retry
// pacing inside runPublishStage. Capped exponential backoff
// (200ms, 400ms, 800ms, ..., 30s) means a sustained broker outage
// produces at most ~2 retry attempts per minute per event, which is
// gentle on the broker once it recovers. ctx cancellation aborts
// the wait so SIGTERM still drains promptly.
const (
	publishBackoffInitial = 200 * time.Millisecond
	publishBackoffMax     = 30 * time.Second
)

// runPublishStage drains normalized events, marshals them into broker
// envelopes, and emits them via the configured Publisher. On Publish
// error, the same event is retried with capped exponential backoff
// until success or ctx cancel — we deliberately do NOT skip on
// failure because doing so silently drops the event while the
// poller continues advancing lastBlock, making the loss undetectable.
//
// The combination that gives us at-least-once delivery end-to-end:
//
//   - The poller-to-publish channel is buffered (capacity 64); when
//     Publish blocks, the channel fills and the poller's
//     `select { case rawLogs <- rl: case <-ctx.Done(): }` backpressures
//     the fetch loop. lastBlock does not advance past unpublished logs.
//   - On a transient Publish error, we retry indefinitely with backoff
//     so the broker outage window is bridged rather than masked.
//   - On ctx cancellation, we return immediately and the parent goroutine
//     drains. In-flight events at shutdown are NOT acknowledged — the
//     watcher refetches them from the chain on the next start (the chain
//     is the source of truth; Kafka is the durable copy).
func (w *Watcher) runPublishStage(ctx context.Context, events <-chan *domain.WalletEvent) {
	for event := range events {
		env := eventToEnvelope(event)
		if !w.publishWithRetry(ctx, event, env) {
			// ctx cancelled mid-publish; the next watcher start will
			// re-fetch from the chain.
			return
		}
		log.Printf("watcher: published event id=%s tx=%s wallet=%s direction=%s token=%s amount=%s",
			env.EventID, truncate(event.TxHash, 10), event.WalletID,
			event.Direction, event.TokenSymbol, event.Amount)
	}
	log.Println("watcher: publish stage done")
}

// publishWithRetry attempts Publish repeatedly with capped exponential
// backoff. Returns true on success, false if ctx is cancelled before
// success.
func (w *Watcher) publishWithRetry(ctx context.Context, event *domain.WalletEvent, env *broker.EventEnvelope) bool {
	backoff := publishBackoffInitial
	attempt := 0
	for {
		attempt++
		err := w.publisher.Publish(ctx, env)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		log.Printf("watcher: publish attempt %d failed (will retry in %s): tx=%s wallet=%s err=%v",
			attempt, backoff, truncate(event.TxHash, 10), event.WalletID, err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < publishBackoffMax {
			backoff *= 2
			if backoff > publishBackoffMax {
				backoff = publishBackoffMax
			}
		}
	}
}

// eventToEnvelope converts a normalized domain event into the wire
// envelope the broker bus expects. EventID is deterministic over
// (network, tx_hash, log_index, wallet_id, direction) so an
// at-least-once re-delivery from upstream produces the same id, and
// downstream consumers can dedupe on it without a full payload
// comparison.
//
// log_index is what makes the EventID unique within a single
// transaction — a multi-hop swap or aggregator tx can emit multiple
// Transfer logs hitting the same wallet/direction; without log_index,
// those would collide and the persister's ON CONFLICT (id) DO NOTHING
// would silently drop the duplicates.
func eventToEnvelope(e *domain.WalletEvent) *broker.EventEnvelope {
	const network = "ethereum" // watcher only handles Ethereum today
	return &broker.EventEnvelope{
		EventID:         makeEventID(network, e.TxHash, e.LogIndex, e.WalletID, e.Direction),
		SchemaVersion:   broker.SchemaCurrent,
		Network:         network,
		EventType:       e.EventType,
		Direction:       e.Direction,
		WalletID:        e.WalletID,
		TokenSymbol:     e.TokenSymbol,
		ContractAddress: e.ContractAddress,
		Amount:          e.Amount,
		TxHash:          e.TxHash,
		BlockNumber:     e.BlockNumber,
		EmittedAt:       time.Now().UTC(),
	}
}

// makeEventID returns a stable id derived from the natural key of a
// wallet event. SHA-256-truncated to 16 hex chars (64 bits) — plenty of
// uniqueness for our scale and short enough to read in log lines.
//
// logIndex disambiguates multiple logs in the same transaction. Empty
// values are accepted (some test fixtures don't set it) but production
// envelopes derived from real eth_getLogs always carry it.
func makeEventID(network, txHash, logIndex, walletID, direction string) string {
	h := sha256.Sum256([]byte(network + "|" + txHash + "|" + logIndex + "|" + walletID + "|" + direction))
	return "evt_" + hex.EncodeToString(h[:8])
}

// startPoller creates a goroutine that periodically fetches logs from Ethereum
// and sends them into the returned channel.
//
// CHANNEL OWNERSHIP: startPoller creates the channel and closes it when done.
// The returned type is <-chan (receive-only) — callers can only read.
//
// SELECT MULTIPLEXING: the poller uses select to handle three signals:
//   - ticker.C: time to poll for new logs
//   - heartbeat.C: time to log a status message (liveness check)
//   - ctx.Done(): shutdown signal
func (w *Watcher) startPoller(ctx context.Context, addresses []string) <-chan RawLog {
	// BUFFER STRATEGY: capacity 64 absorbs bursts when a block contains
	// many Transfer events. Without buffering, the poller would block
	// until the normalizer stage reads each log, slowing down the poll cycle.
	rawLogs := make(chan RawLog, 64)

	go func() {
		// The producer closes the channel — this signals to all downstream
		// stages that no more data is coming.
		defer close(rawLogs)

		ticker := time.NewTicker(w.pollInterval)
		defer ticker.Stop()

		// Heartbeat: periodic log even when no events arrive.
		// Helps operators confirm the watcher is alive.
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()

		var lastBlock uint64

		for {
			// =================================================================
			// ADVANCED SELECT — multiplexing three channels:
			//
			// 1. ticker.C: poll for new logs
			// 2. heartbeat.C: log liveness status
			// 3. ctx.Done(): graceful shutdown
			//
			// Only ONE case executes per iteration. If multiple are ready,
			// Go picks one pseudo-randomly — this is by design to prevent
			// starvation of any case.
			// =================================================================
			select {
			case <-ticker.C:
				logs, newBlock, err := w.logsFetcher.FetchLogs(ctx, addresses, lastBlock)
				if err != nil {
					log.Printf("watcher: fetch logs error: %v", err)
					continue // don't crash on transient errors — retry next tick
				}

				for _, rawJSON := range logs {
					var rl RawLog
					if err := json.Unmarshal(rawJSON, &rl); err != nil {
						log.Printf("watcher: unmarshal log: %v", err)
						continue
					}

					// Send to channel, but respect shutdown.
					// Without this inner select, the goroutine could block
					// on a full channel even after context cancellation.
					select {
					case rawLogs <- rl:
					case <-ctx.Done():
						return
					}
				}

				if newBlock > lastBlock {
					lastBlock = newBlock
					log.Printf("watcher: processed up to block %d, found %d logs", lastBlock, len(logs))
				}

			case <-heartbeat.C:
				log.Printf("watcher: heartbeat — last block: %d, tracking %d addresses",
					lastBlock, len(addresses))

			case <-ctx.Done():
				log.Println("watcher: poller stopping")
				return
			}
		}
	}()

	return rawLogs
}

// (Class 1's persistWorker, logWorker, and metricsWorker were removed
// in Class 2. Persistence moved to cmd/event-persister, ad-hoc logging
// moved to event-router (or any consumer), and metrics moved to
// cmd/event-analytics — each subscribed to the same Kafka topic via
// its own consumer group. See internal/broker/event.go for the topology
// diagram.)

// loadTrackedAddresses builds a map of lowercase Ethereum addresses → wallet IDs.
func (w *Watcher) loadTrackedAddresses(ctx context.Context) (map[string]string, error) {
	wallets, err := w.walletRepo.FindByBlockchain(ctx, "ethereum")
	if err != nil {
		return nil, err
	}

	tracked := make(map[string]string, len(wallets))
	for _, wallet := range wallets {
		tracked[strings.ToLower(wallet.Address)] = wallet.ID
	}

	return tracked, nil
}

// truncate safely shortens a string for logging.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
