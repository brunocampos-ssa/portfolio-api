package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Event Watcher — Ethereum ERC-20 Transfer Monitor
// =============================================================================
//
// Class 2: this binary now produces events onto a Kafka topic instead
// of persisting them itself. Persistence, alerting, and analytics are
// split into separate consumer-group binaries (cmd/event-persister,
// cmd/event-router, cmd/event-analytics).
//
// The watcher still talks to Postgres because it needs to load the set
// of tracked wallet addresses. It does NOT touch wallet_events.
//
// Usage:
//   go run ./cmd/event-watcher
//
// Environment variables:
//   DATABASE_URL          - PostgreSQL connection string
//   ETH_RPC_URL           - Ethereum JSON-RPC endpoint
//   WATCHER_POLL_INTERVAL - Polling interval (e.g., "15s", "1m")
//   WATCHER_START_BLOCK   - Block number to start from (0 = latest)
//   KAFKA_BROKERS         - Comma-separated Kafka bootstrap server list

func main() {
	// --- Configuration ---
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	// --- Database (wallet metadata only — no event persistence) ---
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("FATAL: open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("FATAL: ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// --- Kafka publisher ---
	publisher, err := brokerkafka.NewPublisher(cfg.KafkaBrokers, broker.KafkaTopicWalletEvents)
	if err != nil {
		log.Fatalf("FATAL: kafka publisher: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("warning: kafka publisher close: %v", err)
		}
	}()
	log.Printf("Kafka publisher ready: brokers=%v topic=%s", cfg.KafkaBrokers, broker.KafkaTopicWalletEvents)

	// --- Dependencies ---
	walletRepo := postgres.NewWalletRepository(db)
	logsFetcher := blockchain.NewEthereumLogsFetcher(cfg.EthRPCURL)

	// --- Watcher ---
	w := watcher.NewWatcher(logsFetcher, walletRepo, publisher, cfg.WatcherPollInterval)

	// --- Graceful shutdown ---
	// SIGINT (Ctrl+C) and SIGTERM (Docker stop) cancel the root context;
	// cancellation cascades through poller → normalizer → publish stage.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Starting event watcher...")
	log.Printf("  ETH RPC:        %s", cfg.EthRPCURL)
	log.Printf("  Poll interval:  %s", cfg.WatcherPollInterval)
	log.Printf("  Start block:    %d (0 = latest)", cfg.WatcherStartBlock)
	log.Printf("  Kafka brokers:  %v", cfg.KafkaBrokers)

	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("FATAL: watcher error: %v", err)
	}

	log.Println("Event watcher stopped.")
}
