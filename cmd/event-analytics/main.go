// event-analytics is a Kafka consumer (group base: analytics) that
// maintains in-memory aggregations across the event stream — count
// and amount-total per (network, direction, token, hour). It replaces
// Class 1's in-process metricsWorker, lifted out of the watcher so
// it can be scaled, restarted, and reasoned about independently.
//
// Why a separate consumer group? Because analytics REPLAYS history
// on startup. Every restart recomputes the running totals from t=0;
// the persister and router don't want that behaviour because their
// jobs require resuming from where they left off. Same topic, three
// different read patterns — that's the headline Kafka teaching
// moment of Class 2 made concrete.
//
// Replay semantics are not a kafka.Reader flag: they're a recipe.
//
//   1. Unique group ID per process: kafka.NewReplayConsumer appends
//      a nano suffix to the base "wallet-events-analytics" so each
//      restart is a brand-new group. Kafka's StartOffset=FirstOffset
//      auto-applies to fresh groups, giving us replay-from-zero for
//      free.
//
//   2. No offset commits: the kafka.Consumer skips CommitMessages
//      in replay mode. Committing offsets that nobody will ever
//      resume is wasted broker writes and misleading in dashboards.
//
// Architecture:
//
//   wallet.events.v1 (Kafka topic)
//        │
//        ▼  consumer group: wallet-events-analytics-<nanos> (unique per process)
//   ┌────────────────────┐
//   │  kafka.ReplayCons. │  StartOffset=FirstOffset, no commits
//   └─────────┬──────────┘
//             │  *broker.EventEnvelope
//             ▼
//   ┌────────────────────┐
//   │ analytics.Aggrega. │  in-memory map[(net,dir,token,hour)] -> {count, total}
//   └─────────┬──────────┘
//             ▼ (every ANALYTICS_DUMP_INTERVAL)
//        stdout dump
//
// The aggregator's state is ephemeral by design — crash the process
// and the numbers vanish. That's fine because Kafka is the source of
// truth: the next startup rebuilds them deterministically from the
// log. This is the "derive views from the log" pattern in miniature.
//
// Usage:
//   go run ./cmd/event-analytics
//
// Environment variables:
//   KAFKA_BROKERS            - Comma-separated Kafka bootstrap server list
//   ANALYTICS_DUMP_INTERVAL  - How often to dump aggregates (default 30s)
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/analytics"
	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
)

const defaultDumpInterval = 30 * time.Second

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	dumpInterval := parseDumpInterval(os.Getenv("ANALYTICS_DUMP_INTERVAL"))

	consumer, err := brokerkafka.NewReplayConsumer(
		cfg.KafkaBrokers,
		broker.KafkaTopicWalletEvents,
		broker.KafkaGroupAnalytics,
	)
	if err != nil {
		log.Fatalf("FATAL: kafka replay consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("warning: kafka consumer close: %v", err)
		}
	}()

	agg := analytics.NewAggregator()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Periodic dump: a separate goroutine prints the current snapshot
	// every dumpInterval. Cancellation drains the goroutine first so
	// the final dump after shutdown is preserved in logs.
	dumpDone := make(chan struct{})
	go runDumper(ctx, agg, dumpInterval, dumpDone)

	log.Println("Starting event analytics...")
	log.Printf("  Kafka brokers:   %v", cfg.KafkaBrokers)
	log.Printf("  Topic:           %s", broker.KafkaTopicWalletEvents)
	log.Printf("  Group base:      %s (unique suffix appended per process)", broker.KafkaGroupAnalytics)
	log.Printf("  Dump interval:   %s", dumpInterval)
	log.Println("  Replay mode:    enabled — recomputes from offset 0, never commits")

	if err := consumer.Run(ctx, agg.Apply); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("FATAL: consumer error: %v", err)
	}

	<-dumpDone
	// Final dump on the way out so operators see the last numbers.
	if _, err := agg.Dump(os.Stdout); err != nil {
		log.Printf("warning: final analytics dump: %v", err)
	}
	log.Println("Event analytics stopped.")
}

// runDumper prints aggregate snapshots on a ticker until ctx is done.
// Signals completion via dumpDone so main can sequence a final dump
// after the goroutine exits cleanly.
func runDumper(ctx context.Context, agg *analytics.Aggregator, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := agg.Dump(os.Stdout); err != nil {
				log.Printf("warning: analytics dump: %v", err)
			}
		}
	}
}

// parseDumpInterval returns the user-configured cadence, falling back
// to the default if the value is empty or unparseable. Local helper
// rather than reaching into config because this knob is binary-
// specific — no other process consumes ANALYTICS_DUMP_INTERVAL.
func parseDumpInterval(raw string) time.Duration {
	if raw == "" {
		return defaultDumpInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Printf("warning: invalid ANALYTICS_DUMP_INTERVAL %q, using default %s", raw, defaultDumpInterval)
		return defaultDumpInterval
	}
	return d
}
