package snapshot

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Snapshot Runner — Wallet Balance Aggregator
// =============================================================================
//
// The Runner generates a point-in-time snapshot of all tracked ETH wallets,
// simulating the basis of a tax report. It fetches native ETH balance plus
// selected ERC-20 token balances, converts everything to USD, and persists
// the results.
//
// Pipeline architecture:
//
//   ┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────┐
//   │  Load ETH   │ ──► │   Generate   │ ──► │  Worker Pool │ ──► │ Aggregate │
//   │  wallets    │     │   tasks      │     │  (bounded)   │     │ & persist │
//   └─────────────┘     └──────────────┘     └──────────────┘     └───────────┘
//        Stage 1              Stage 2              Stage 3            Stage 4
//      (database)        (chan wallet)       (fan-out/fan-in)      (collect all)
//
// CONCURRENCY CONCEPTS DEMONSTRATED:
//
// 1. Worker pool with bounded concurrency:
//    - N workers process wallets concurrently
//    - Prevents overwhelming the Ethereum RPC endpoint
//    - Each worker fetches ETH + token balances + USD prices
//
// 2. Pipeline stages:
//    - Stage 1: Load wallets (synchronous, database query)
//    - Stage 2: Generate channel (convert slice → channel)
//    - Stage 3: Worker pool (concurrent processing)
//    - Stage 4: Collect results (aggregate into snapshot)
//
// 3. Fan-in:
//    - Multiple workers write results to a single output channel
//    - The aggregator reads from this single channel
//
// 4. Buffer strategies:
//    - Input channel: buffered with len(wallets) — all items fit at once
//    - Output channel: buffered with numWorkers — max in-flight results
//    - Chosen to minimize blocking while limiting memory usage

// tokenConfig defines an ERC-20 token to include in snapshots.
type tokenConfig struct {
	Symbol          string
	ContractAddress string
	Decimals        int
}

// defaultTokens lists ERC-20 tokens included in every snapshot.
// These are well-known stablecoins that appear frequently in ETH wallets.
var defaultTokens = []tokenConfig{
	{Symbol: "USDC", ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", Decimals: 6},
	{Symbol: "USDT", ContractAddress: "0xdac17f958d2ee523a2206206994597c13d831ec7", Decimals: 6},
}

// walletResult holds the processing result for a single wallet.
// It contains all snapshot items (native + tokens) for that wallet.
type walletResult struct {
	WalletID string
	Items    []domain.WalletSnapshotItem
	Error    error
}

// Runner orchestrates the snapshot generation process.
type Runner struct {
	walletRepo    contracts.WalletRepository
	snapshotRepo  contracts.SnapshotRepository
	ethProvider   contracts.BalanceProvider
	tokenProvider contracts.TokenBalanceProvider
	priceProvider contracts.PriceProvider
	numWorkers    int
}

// NewRunner creates a snapshot runner with all dependencies injected.
func NewRunner(
	walletRepo contracts.WalletRepository,
	snapshotRepo contracts.SnapshotRepository,
	ethProvider contracts.BalanceProvider,
	tokenProvider contracts.TokenBalanceProvider,
	priceProvider contracts.PriceProvider,
	numWorkers int,
) *Runner {
	if walletRepo == nil {
		panic("snapshot.NewRunner: walletRepo must not be nil")
	}
	if snapshotRepo == nil {
		panic("snapshot.NewRunner: snapshotRepo must not be nil")
	}
	if ethProvider == nil {
		panic("snapshot.NewRunner: ethProvider must not be nil")
	}
	if priceProvider == nil {
		panic("snapshot.NewRunner: priceProvider must not be nil")
	}
	if numWorkers <= 0 {
		numWorkers = 3
	}
	return &Runner{
		walletRepo:    walletRepo,
		snapshotRepo:  snapshotRepo,
		ethProvider:   ethProvider,
		tokenProvider: tokenProvider,
		priceProvider: priceProvider,
		numWorkers:    numWorkers,
	}
}

// Run executes the full snapshot pipeline:
//  1. Load ETH wallets from the database
//  2. Create a "pending" snapshot record
//  3. Process wallets concurrently using a worker pool
//  4. Aggregate results and persist snapshot items
//  5. Mark snapshot as "completed" or "failed"
func (r *Runner) Run(ctx context.Context) (*domain.WalletSnapshot, error) {
	start := time.Now()
	log.Println("snapshot: starting snapshot generation...")

	// =========================================================================
	// STAGE 1: Load wallets from the database
	// =========================================================================
	wallets, err := r.walletRepo.FindByBlockchain(ctx, "ethereum")
	if err != nil {
		return nil, fmt.Errorf("snapshot: load wallets: %w", err)
	}

	if len(wallets) == 0 {
		log.Println("snapshot: no ethereum wallets found")
		return nil, fmt.Errorf("snapshot: no ethereum wallets to process")
	}

	log.Printf("snapshot: found %d ethereum wallets, using %d workers", len(wallets), r.numWorkers)

	// =========================================================================
	// Create the snapshot record in "pending" status.
	// =========================================================================
	snapshot := &domain.WalletSnapshot{
		ID:            fmt.Sprintf("snap%d", time.Now().UnixNano()),
		ReferenceTime: time.Now(),
		Status:        "pending",
	}

	if err := r.snapshotRepo.CreateSnapshot(ctx, snapshot); err != nil {
		return nil, fmt.Errorf("snapshot: create record: %w", err)
	}

	// =========================================================================
	// STAGE 2: Generate — convert wallet slice into a channel
	//
	// concurrent.Generate creates a channel, sends all items, and closes it.
	// This converts our in-memory slice into a stream that the worker pool
	// can consume.
	//
	// BUFFER STRATEGY: the channel is buffered with len(wallets).
	// Since we know the total count upfront, all items are sent immediately
	// without blocking. This is appropriate for bounded, known-size inputs.
	// =========================================================================
	walletCh := concurrent.Generate(ctx, wallets)

	// =========================================================================
	// STAGE 3: Worker Pool — bounded concurrent processing
	//
	// RunWorkerPool starts exactly numWorkers goroutines. Each worker:
	//   1. Reads a wallet from the input channel
	//   2. Fetches native ETH balance
	//   3. Fetches ERC-20 token balances (USDC, USDT)
	//   4. Looks up USD prices for each asset
	//   5. Sends the result to the output channel
	//
	// FAN-OUT: multiple workers read from the same input channel.
	// Go guarantees each wallet goes to exactly one worker.
	//
	// FAN-IN: all workers write to the same output channel.
	// The consumer (stage 4) reads from this single channel.
	//
	// BOUNDED CONCURRENCY: with numWorkers=3, at most 3 RPC calls
	// happen simultaneously. This prevents rate limiting by the
	// Ethereum RPC provider.
	// =========================================================================
	resultCh := concurrent.RunWorkerPool(ctx, r.numWorkers, walletCh, func(ctx context.Context, wallet domain.Wallet) walletResult {
		return r.processWallet(ctx, wallet, snapshot.ID)
	})

	// =========================================================================
	// STAGE 4: Aggregate — collect all results
	//
	// We read from the output channel until it closes (all workers done).
	// This is the fan-in consumer: one reader, many writers.
	// =========================================================================
	var allItems []domain.WalletSnapshotItem
	var errors []string

	for result := range resultCh {
		if result.Error != nil {
			log.Printf("snapshot: wallet %s error: %v", result.WalletID, result.Error)
			errors = append(errors, fmt.Sprintf("wallet %s: %v", result.WalletID, result.Error))
			continue
		}
		allItems = append(allItems, result.Items...)
	}

	// =========================================================================
	// Persist snapshot items and update status.
	// =========================================================================
	for i := range allItems {
		if err := r.snapshotRepo.CreateSnapshotItem(ctx, &allItems[i]); err != nil {
			log.Printf("snapshot: persist item error: %v", err)
		}
	}

	// Determine final status.
	status := "completed"
	if len(errors) > 0 && len(allItems) == 0 {
		status = "failed"
	}

	if err := r.snapshotRepo.UpdateSnapshotStatus(ctx, snapshot.ID, status); err != nil {
		log.Printf("snapshot: update status error: %v", err)
	}

	snapshot.Status = status
	snapshot.Items = allItems

	log.Printf("snapshot: completed in %s — %d items, %d errors, status=%s",
		time.Since(start), len(allItems), len(errors), status)

	return snapshot, nil
}

// processWallet fetches all balances for a single wallet.
// This function runs inside a worker goroutine.
//
// It demonstrates partial failure: if one token balance fails,
// we still include the native balance and other successful tokens.
func (r *Runner) processWallet(ctx context.Context, wallet domain.Wallet, snapshotID string) walletResult {
	start := time.Now()
	defer func() {
		log.Printf("snapshot: processed wallet %s in %s", wallet.Address[:min(8, len(wallet.Address))], time.Since(start))
	}()

	var items []domain.WalletSnapshotItem

	// Fetch native ETH balance.
	ethBalance, asset, err := r.ethProvider.GetBalance(ctx, wallet.Address)
	if err != nil {
		return walletResult{WalletID: wallet.ID, Error: fmt.Errorf("fetch ETH balance: %w", err)}
	}

	// Fetch ETH price.
	ethPrice, err := r.priceProvider.GetPriceUSD(ctx, asset)
	if err != nil {
		log.Printf("snapshot: ETH price error: %v", err)
		ethPrice = 0
	}

	items = append(items, domain.WalletSnapshotItem{
		ID:          fmt.Sprintf("si%d", time.Now().UnixNano()),
		SnapshotID:  snapshotID,
		WalletID:    wallet.ID,
		AssetSymbol: asset,
		AssetType:   "native",
		Amount:      ethBalance,
		USDPrice:    ethPrice,
		USDValue:    ethBalance * ethPrice,
	})

	// Fetch ERC-20 token balances (if token provider is available).
	if r.tokenProvider != nil {
		for _, token := range defaultTokens {
			balance, err := r.tokenProvider.GetTokenBalance(ctx, wallet.Address, token.ContractAddress, token.Decimals)
			if err != nil {
				log.Printf("snapshot: %s balance error for %s: %v",
					token.Symbol, wallet.Address[:min(8, len(wallet.Address))], err)
				continue // partial failure: skip this token, continue with others
			}

			// Skip zero balances — they add noise without value.
			if balance == 0 {
				continue
			}

			// For stablecoins, we use a fixed price of $1.00.
			// In production, you'd fetch the actual price.
			tokenPrice := 1.0
			if token.Symbol != "USDC" && token.Symbol != "USDT" {
				if p, err := r.priceProvider.GetPriceUSD(ctx, token.Symbol); err == nil {
					tokenPrice = p
				}
			}

			items = append(items, domain.WalletSnapshotItem{
				ID:              fmt.Sprintf("si%d", time.Now().UnixNano()),
				SnapshotID:      snapshotID,
				WalletID:        wallet.ID,
				AssetSymbol:     token.Symbol,
				AssetType:       "erc20",
				ContractAddress: token.ContractAddress,
				Amount:          balance,
				USDPrice:        tokenPrice,
				USDValue:        balance * tokenPrice,
			})
		}
	}

	return walletResult{WalletID: wallet.ID, Items: items}
}
