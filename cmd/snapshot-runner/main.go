package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/pricing"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/snapshot"
)

// =============================================================================
// Snapshot Runner — Wallet Balance Aggregator / Tax Report Simulator
// =============================================================================
//
// This binary generates a point-in-time snapshot of all tracked ETH wallets,
// fetching native ETH and ERC-20 token balances, converting them to USD.
//
// It demonstrates advanced concurrency patterns:
//   - Worker pool with bounded concurrency
//   - Pipeline stages (load → generate → process → aggregate)
//   - Fan-in (multiple workers → single output channel)
//   - Buffer strategies (why each channel is buffered)
//
// Usage:
//   go run ./cmd/snapshot-runner
//
// Environment variables:
//   DATABASE_URL      - PostgreSQL connection string
//   ETH_RPC_URL       - Ethereum JSON-RPC endpoint
//   SNAPSHOT_WORKERS   - Number of concurrent workers (default: 3)
//   COINGECKO_DEMO_API_KEY - CoinGecko API key (optional, uses mock if empty)

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
	snapshotRepo := postgres.NewSnapshotRepository(db)
	ethProvider := blockchain.NewEthereumProvider(cfg.EthRPCURL)
	tokenProvider := blockchain.NewEthereumTokenProvider(cfg.EthRPCURL)

	// Price provider: real or mock.
	var priceProvider contracts.PriceProvider
	if cfg.CoinGeckoAPIKey != "" {
		priceProvider = pricing.NewCoinGeckoPriceProvider(cfg.CoinGeckoAPIKey)
		log.Println("Price provider: CoinGecko (live)")
	} else {
		priceProvider = pricing.NewMockPriceProvider()
		log.Println("Price provider: Mock (COINGECKO_DEMO_API_KEY not set)")
	}

	// --- Snapshot Runner ---
	runner := snapshot.NewRunner(
		walletRepo,
		snapshotRepo,
		ethProvider,
		tokenProvider,
		priceProvider,
		cfg.SnapshotWorkers,
	)

	log.Println("Starting snapshot generation...")
	log.Printf("  ETH RPC:   %s", cfg.EthRPCURL)
	log.Printf("  Workers:   %d", cfg.SnapshotWorkers)

	// Create a context with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Run the snapshot pipeline.
	result, err := runner.Run(ctx)
	if err != nil {
		log.Fatalf("FATAL: snapshot error: %v", err)
	}

	// Print the snapshot result as pretty JSON.
	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Fprintln(os.Stdout, string(output))

	log.Printf("Snapshot %s completed with %d items.", result.ID, len(result.Items))
}
