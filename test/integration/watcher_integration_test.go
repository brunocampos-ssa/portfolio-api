//go:build integration

package integration

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Watcher integration test — forked mainnet + DB-seeded wallets.
//
// testenv.Setup has already:
//
//   - Migrated Postgres (migration 002 seeds two ETH wallets).
//   - Forked mainnet via Anvil (defaults to the RPC's latest block;
//     Options.ForkBlock pins a specific block when supplied and the RPC
//     retains archive state for it).
//   - Seeded each distinct tracked address with 10 ETH + bootstrap tokens.
//
// This test picks one of those addresses, fires a fresh USDC Transfer from
// the whale into it, and verifies that the production watcher detects and
// persists the event exactly as it would in staging.
// =============================================================================

func TestWatcherForkedMainnetUSDCTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	// Pick the first tracked address from the DB. No hardcoding.
	trackedAddrs, err := env.GetTrackedETHAddresses(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, trackedAddrs, "bootstrap must have produced at least one ETH wallet")
	tracked := trackedAddrs[0]

	usdc := env.Fixtures.Tokens["USDC"]

	// Fire a fresh 1-USDC transfer into the tracked wallet. The whale still
	// has plenty of USDC after bootstrap.
	txHash, err := env.TransferERC20(ctx,
		usdc.Contract,
		usdc.Whale,
		tracked,
		big.NewInt(1_000_000), // 1.0 USDC (6 decimals)
	)
	require.NoError(t, err)
	require.NotEmpty(t, txHash)

	// Run the production watcher against the forked chain with a tight poll
	// interval so the test is quick. The watcher loads its tracked set from
	// the DB — same set we queried above.
	walletRepo := postgres.NewWalletRepository(env.DB)
	eventRepo := postgres.NewEventRepository(env.DB)
	logsFetcher := blockchain.NewEthereumLogsFetcher(env.RPCURL())

	w := watcher.NewWatcher(logsFetcher, walletRepo, eventRepo, 500*time.Millisecond)

	watcherCtx, cancelW := context.WithCancel(ctx)
	watcherDone := make(chan error, 1)
	go func() { watcherDone <- w.Run(watcherCtx) }()
	t.Cleanup(func() {
		cancelW()
		select {
		case err := <-watcherDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("watcher exited with: %v", err)
			}
		case <-time.After(5 * time.Second):
			// Failing here surfaces goroutine leaks instead of letting them
			// silently flake the next test in the suite.
			t.Errorf("watcher did not exit within 5s after cancel")
		}
	})

	// Poll `wallet_events` until the row appears, then assert its shape.
	require.Eventually(t, func() bool {
		var cnt int
		if err := env.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM wallet_events
			WHERE tx_hash = $1 AND direction = 'incoming'
		`, txHash).Scan(&cnt); err != nil {
			return false
		}
		return cnt > 0
	}, 30*time.Second, 200*time.Millisecond,
		"watcher should persist the USDC Transfer event")

	// Sanity: verify the stored event is properly normalized.
	var tokenSymbol, direction string
	err = env.DB.QueryRowContext(ctx, `
		SELECT token_symbol, direction FROM wallet_events WHERE tx_hash = $1 LIMIT 1`,
		txHash,
	).Scan(&tokenSymbol, &direction)
	require.NoError(t, err)
	require.Equal(t, "USDC", tokenSymbol)
	require.Equal(t, "incoming", direction)
}
