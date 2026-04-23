//go:build integration

package integration

import (
	"context"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/pricing"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/snapshot"
)

// =============================================================================
// Snapshot integration test — closes the loop between:
//
//   DB seed (migration 002)  →  chain state (testenv.Bootstrap)  →  snapshot pipeline
//
// The env has already read the tracked Ethereum wallets from the DB and
// seeded their balances on the forked chain. Here we run the production
// snapshot runner end-to-end and verify that what it computed matches the
// bootstrap fixtures.
// =============================================================================

func TestSnapshotRunner_AgainstSeededChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	walletRepo := postgres.NewWalletRepository(env.DB)
	snapshotRepo := postgres.NewSnapshotRepository(env.DB)
	ethProvider := blockchain.NewEthereumProvider(env.RPCURL())
	tokenProvider := blockchain.NewEthereumTokenProvider(env.RPCURL())
	priceProvider := pricing.NewMockPriceProvider()

	runner := snapshot.NewRunner(walletRepo, snapshotRepo,
		ethProvider, tokenProvider, priceProvider, 2)

	result, err := runner.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.NotEmpty(t, result.Items, "snapshot should contain items for the seeded wallets")

	// Pull the DB's view of tracked wallets so we can compute the expected
	// shape without hardcoding migration-002 specifics here.
	trackedAddrs, err := env.GetTrackedETHAddresses(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, trackedAddrs)

	nativeByWallet := map[string]float64{}
	tokensByWallet := map[string]map[string]float64{}
	for _, it := range result.Items {
		switch it.AssetType {
		case "native":
			nativeByWallet[it.WalletID] = it.Amount
		case "erc20":
			if tokensByWallet[it.WalletID] == nil {
				tokensByWallet[it.WalletID] = map[string]float64{}
			}
			tokensByWallet[it.WalletID][it.AssetSymbol] = it.Amount
		default:
			t.Fatalf("unexpected asset type: %q", it.AssetType)
		}
	}

	// Every tracked wallet row must have a native item matching the fixture
	// exactly: anvil_setBalance OVERWRITES the account's ETH balance, so
	// whatever was pre-fork is gone.
	expectedETH := env.Fixtures.NativeBalanceETH
	require.Len(t, nativeByWallet, countETHWallets(ctx, t),
		"one native item per ETH wallet row")
	for walletID, amount := range nativeByWallet {
		require.InDeltaf(t, expectedETH, amount, 1e-9,
			"wallet %s: native ETH does not match bootstrap fixture", walletID)
	}

	// Every tracked wallet must carry each fixture token in AT LEAST the
	// bootstrap amount. Unlike native ETH, ERC-20 bootstrap uses a real
	// `transfer` which ADDS on top of the pre-fork balance — if the seeded
	// address is famous (e.g. Vitalik), its mainnet token holdings stay.
	// The test asserts the floor; the ceiling is real-world noise.
	for walletID, tokens := range tokensByWallet {
		for sym, fx := range env.Fixtures.Tokens {
			got, present := tokens[sym]
			require.Truef(t, present,
				"wallet %s missing bootstrap token %s", walletID, sym)
			floor := fixtureTokensToHumanUnits(t, fx.Amount, fx.Decimals)
			require.GreaterOrEqualf(t, got, floor,
				"wallet %s / %s: bootstrap transferred %.2f, got %.2f (regression?)",
				walletID, sym, floor, got)
		}
	}

	// Snapshot row itself was persisted.
	var persistedStatus string
	err = env.DB.QueryRowContext(ctx,
		`SELECT status FROM wallet_snapshots WHERE id = $1`, result.ID,
	).Scan(&persistedStatus)
	require.NoError(t, err)
	require.Equal(t, "completed", persistedStatus)
}

// countETHWallets returns the number of wallet ROWS (not distinct addresses)
// tracked in the DB for blockchain=ethereum. migration 002 seeds two ETH
// wallets sharing one address; the snapshot emits one native item per row.
func countETHWallets(ctx context.Context, t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, env.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM wallets WHERE blockchain = 'ethereum'`,
	).Scan(&n))
	return n
}

// fixtureTokensToHumanUnits turns a raw uint256 amount ("1000000000") plus
// decimals (6) into a float for comparison with the snapshot's Amount
// (which the provider already scales by 10^decimals).
//
// Malformed fixture data (e.g. non-decimal Amount) trips t.Fatalf instead
// of a confusing nil-deref panic deep in big.Float. Loader validation in
// testenv should also catch this earlier.
func fixtureTokensToHumanUnits(t *testing.T, rawAmount string, decimals int) float64 {
	t.Helper()
	raw, ok := new(big.Int).SetString(rawAmount, 10)
	if !ok {
		t.Fatalf("invalid fixture amount %q (expected base-10 uint256)", rawAmount)
	}
	f := new(big.Float).SetInt(raw)
	f.Quo(f, big.NewFloat(math.Pow10(decimals)))
	v, _ := f.Float64()
	return v
}
