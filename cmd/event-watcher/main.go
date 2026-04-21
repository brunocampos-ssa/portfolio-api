package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Event Watcher — Ethereum ERC-20 Transfer Monitor
// =============================================================================
//
// This binary monitors the Ethereum blockchain for ERC-20 Transfer events
// involving wallets tracked in the PostgreSQL database.
//
// It demonstrates advanced concurrency patterns:
//   - Directional channels (chan<-, <-chan)
//   - Pipeline stages with channel ownership
//   - select with ctx.Done(), ticker, and heartbeat
//   - Fan-out (broadcast) to multiple consumers
//   - Graceful shutdown via OS signal handling
//
// Usage:
//   go run ./cmd/event-watcher
//
// Environment variables:
//   DATABASE_URL         - PostgreSQL connection string
//   ETH_RPC_URL          - Ethereum JSON-RPC endpoint
//   WATCHER_POLL_INTERVAL - Polling interval (e.g., "15s", "1m")
//   WATCHER_START_BLOCK  - Block number to start from (0 = latest)

func main() {
	// --- Configuration ---
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	// --- Database ---
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("FATAL: open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("FATAL: ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// --- Dependencies ---
	walletRepo := postgres.NewWalletRepository(db)
	eventRepo := postgres.NewEventRepository(db)
	logsFetcher := blockchain.NewEthereumLogsFetcher(cfg.EthRPCURL)

	// --- Watcher ---
	w := watcher.NewWatcher(logsFetcher, walletRepo, eventRepo, cfg.WatcherPollInterval)

	// --- Graceful shutdown ---
	// Create a context that is cancelled when the process receives
	// SIGINT (Ctrl+C) or SIGTERM (Docker stop).
	// This context propagates cancellation through the entire pipeline:
	// poller → normalizer → fan-out → consumers
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Starting event watcher...")
	log.Printf("  ETH RPC:        %s", cfg.EthRPCURL)
	log.Printf("  Poll interval:  %s", cfg.WatcherPollInterval)
	log.Printf("  Start block:    %d (0 = latest)", cfg.WatcherStartBlock)

	// Run blocks until context is cancelled.
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("FATAL: watcher error: %v", err)
	}

	log.Println("Event watcher stopped.")
}
