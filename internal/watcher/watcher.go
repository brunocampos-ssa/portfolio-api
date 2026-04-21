package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

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
// Architecture (data flow through channels):
//
//   ┌──────────────┐     ┌─────────────┐     ┌──────────────────┐
//   │  eth_getLogs  │ ──► │  rawLogs    │ ──► │  normalizer      │
//   │  (poller)     │     │  chan RawLog │     │  (pipeline stage)│
//   └──────────────┘     └─────────────┘     └───────┬──────────┘
//                                                     │
//                                              normalized events
//                                                     │
//                                              ┌──────▼──────┐
//                                              │   fan-out    │
//                                              └──────┬──────┘
//                                         ┌───────────┼───────────┐
//                                         ▼           ▼           ▼
//                                    ┌─────────┐ ┌─────────┐ ┌─────────┐
//                                    │ persist │ │  logger  │ │ metrics │
//                                    └─────────┘ └─────────┘ └─────────┘
//
// CONCURRENCY CONCEPTS DEMONSTRATED:
//
// 1. Directional channels (<-chan, chan<-):
//    - startPoller() returns <-chan RawLog (receive-only for consumers)
//    - Pipeline Stage returns <-chan *WalletEvent
//    - FanOut returns []<-chan *WalletEvent
//    Direction enforces correct usage at compile time.
//
// 2. Channel ownership and close semantics:
//    - The poller creates and closes rawLogs
//    - The pipeline stage creates and closes normalized
//    - FanOut creates and closes consumer channels
//    Rule: only the producer (owner) closes a channel.
//
// 3. Advanced select:
//    - Multiplexing between ticker, heartbeat, and ctx.Done()
//    - The metrics worker reads from both events and a timer
//
// 4. Fan-out (broadcast):
//    - Each normalized event is sent to ALL consumers
//    - persist, log, and metrics workers each see every event
//
// 5. Graceful shutdown:
//    - Context cancellation propagates through all stages
//    - Each stage checks ctx.Done() before blocking operations

// Watcher is the main event monitoring service.
type Watcher struct {
	logsFetcher  contracts.LogsFetcher
	walletRepo   contracts.WalletRepository
	eventRepo    contracts.EventRepository
	pollInterval time.Duration
}

// NewWatcher creates a new event watcher.
//
// Dependencies:
//   - logsFetcher: polls Ethereum for event logs (eth_getLogs)
//   - walletRepo: loads tracked wallet addresses from the database
//   - eventRepo: persists detected events to the database
//   - pollInterval: how often to poll for new logs
func NewWatcher(
	logsFetcher contracts.LogsFetcher,
	walletRepo contracts.WalletRepository,
	eventRepo contracts.EventRepository,
	pollInterval time.Duration,
) *Watcher {
	if logsFetcher == nil {
		panic("watcher.NewWatcher: logsFetcher must not be nil")
	}
	if walletRepo == nil {
		panic("watcher.NewWatcher: walletRepo must not be nil")
	}
	if eventRepo == nil {
		panic("watcher.NewWatcher: eventRepo must not be nil")
	}
	return &Watcher{
		logsFetcher:  logsFetcher,
		walletRepo:   walletRepo,
		eventRepo:    eventRepo,
		pollInterval: pollInterval,
	}
}

// Run starts the watcher. It blocks until the context is cancelled.
//
// The data flows through a pipeline of channels:
// 1. Poller produces raw logs → rawLogs channel
// 2. Pipeline Stage normalizes logs → normalized channel
// 3. FanOut broadcasts to consumers → 3 consumer channels
func (w *Watcher) Run(ctx context.Context) error {
	log.Println("watcher: loading tracked wallets...")

	// Build a lookup map: lowercase address → wallet ID.
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

	// Extract addresses for the log filter.
	addresses := make([]string, 0, len(trackedAddresses))
	for addr := range trackedAddresses {
		addresses = append(addresses, addr)
	}

	// =========================================================================
	// STAGE 1: Poller
	//
	// The poller goroutine periodically calls eth_getLogs and sends raw log
	// entries into the rawLogs channel.
	//
	// CHANNEL OWNERSHIP: startPoller creates AND closes rawLogs.
	// It returns <-chan RawLog (receive-only) so consumers can't close it.
	// =========================================================================
	rawLogs := w.startPoller(ctx, addresses)

	// =========================================================================
	// STAGE 2: Normalizer (Pipeline Stage)
	//
	// Transforms raw Ethereum logs into domain WalletEvent objects.
	// Uses concurrent.Stage — a reusable pipeline building block.
	//
	// The transform returns (event, keep):
	//   - keep=true: event is a relevant Transfer for a tracked wallet
	//   - keep=false: event is irrelevant (wrong type, not tracked, etc.)
	//
	// CLOSE PROPAGATION: when rawLogs closes, the stage finishes processing
	// and closes its output. This creates a cascade through the pipeline.
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
	// STAGE 3: Fan-Out (Broadcast)
	//
	// Each normalized event is broadcast to ALL 3 consumers.
	// This is different from a worker pool where each item goes to ONE worker.
	//
	// Use case: the same event must be persisted, logged, AND counted.
	// Each consumer operates independently — a slow persister doesn't block
	// the logger (thanks to buffered channels inside FanOut).
	// =========================================================================
	consumers := concurrent.FanOut(ctx, normalized, 3)

	// Start consumer goroutines.
	// Each consumer reads from its own channel — it does NOT close it.
	// The channels are closed by FanOut when the input is exhausted.
	go w.persistWorker(ctx, consumers[0])
	go w.logWorker(ctx, consumers[1])
	go w.metricsWorker(ctx, consumers[2])

	log.Println("watcher: running — press Ctrl+C to stop")

	// Block until context is cancelled (e.g., by SIGINT/SIGTERM).
	<-ctx.Done()
	log.Println("watcher: shutting down gracefully...")
	return ctx.Err()
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

// persistWorker saves events to the database.
//
// IMPORTANT: this is a CONSUMER — it does NOT close the channel.
// The channel is owned by FanOut, which closes it when the input is exhausted.
// Closing a channel from the consumer side would cause a panic if the producer
// tries to send to it.
func (w *Watcher) persistWorker(ctx context.Context, events <-chan *domain.WalletEvent) {
	for event := range events {
		if err := w.eventRepo.Create(ctx, event); err != nil {
			log.Printf("watcher: persist error: tx=%s err=%v",
				truncate(event.TxHash, 10), err)
			continue
		}
		log.Printf("watcher: persisted event tx=%s wallet=%s direction=%s",
			truncate(event.TxHash, 10), event.WalletID, event.Direction)
	}
	log.Println("watcher: persist worker done")
}

// logWorker prints events for observability.
func (w *Watcher) logWorker(_ context.Context, events <-chan *domain.WalletEvent) {
	for event := range events {
		log.Printf("watcher: [EVENT] type=%s direction=%s token=%s amount=%s tx=%s block=%d",
			event.EventType, event.Direction, event.TokenSymbol,
			event.Amount, truncate(event.TxHash, 10), event.BlockNumber)
	}
	log.Println("watcher: log worker done")
}

// metricsWorker counts events by direction for basic monitoring.
//
// This worker demonstrates a more complex select pattern: it reads from
// TWO channels simultaneously — the events channel and a periodic ticker.
func (w *Watcher) metricsWorker(ctx context.Context, events <-chan *domain.WalletEvent) {
	incoming := 0
	outgoing := 0

	// Periodic metrics reporting.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		// =================================================================
		// SELECT WITH MULTIPLE SOURCES:
		//
		// This select reads from three different channels:
		// 1. events: count each incoming/outgoing event
		// 2. ticker.C: periodically report accumulated metrics
		// 3. ctx.Done(): clean shutdown
		//
		// NOTE: when events is closed, the receive returns (nil, false).
		// We detect this with the ok idiom and exit gracefully.
		// =================================================================
		select {
		case event, ok := <-events:
			if !ok {
				// Channel closed — report final metrics and exit.
				log.Printf("watcher: [METRICS] final — incoming=%d outgoing=%d total=%d",
					incoming, outgoing, incoming+outgoing)
				return
			}
			switch event.Direction {
			case "incoming":
				incoming++
			case "outgoing":
				outgoing++
			}

		case <-ticker.C:
			log.Printf("watcher: [METRICS] incoming=%d outgoing=%d total=%d",
				incoming, outgoing, incoming+outgoing)

		case <-ctx.Done():
			log.Printf("watcher: [METRICS] shutdown — incoming=%d outgoing=%d total=%d",
				incoming, outgoing, incoming+outgoing)
			return
		}
	}
}

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
