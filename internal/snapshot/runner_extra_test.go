package snapshot_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/snapshot"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/mocks"
)

// =============================================================================
// Extra runner tests using testify/mock at the infrastructure boundary.
//
// These complement the hand-rolled table tests in runner_test.go. They focus
// on call orchestration and error propagation (where `mock` shines) rather
// than value checks (where plain structs read better).
// =============================================================================

// TestRunner_Run_LoadWalletsError_Propagates ensures repository errors bubble
// up wrapped with %w so errors.Is still works.
func TestRunner_Run_LoadWalletsError_Propagates(t *testing.T) {
	boom := errors.New("db exploded")

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").
		Return([]domain.Wallet(nil), boom)

	snapshotRepo := &mocks.SnapshotRepository{}
	ethProvider := &mocks.BalanceProvider{}
	priceProvider := &mocks.PriceProvider{}

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 2)

	result, err := runner.Run(t.Context())
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, boom)

	// Snapshot record was never created — we fail before persisting.
	snapshotRepo.AssertNotCalled(t, "CreateSnapshot", mock.Anything, mock.Anything)
}

// TestRunner_Run_BalanceError_MarksFailed verifies that when every wallet
// errors on balance fetch, the snapshot ends in "failed" status.
func TestRunner_Run_BalanceError_MarksFailed(t *testing.T) {
	wallets := []domain.Wallet{
		{ID: "w1", Blockchain: "ethereum", Address: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").Return(wallets, nil)

	snapshotRepo := &mocks.SnapshotRepository{}
	snapshotRepo.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("UpdateSnapshotStatus", mock.Anything, mock.Anything, "failed").Return(nil)

	ethProvider := &mocks.BalanceProvider{}
	ethProvider.On("GetBalance", mock.Anything, mock.Anything).
		Return(0.0, "ETH", errors.New("upstream 500"))

	priceProvider := &mocks.PriceProvider{}

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 1)

	result, err := runner.Run(t.Context())
	require.NoError(t, err) // Run itself succeeds; status captures the failure.
	require.Equal(t, "failed", result.Status)
	require.Empty(t, result.Items)

	snapshotRepo.AssertExpectations(t)
}

// TestRunner_Run_PartialPriceFailure_StillCompletes verifies that a missing
// price for the native asset doesn't abort the snapshot — USDValue just
// collapses to zero for that item.
func TestRunner_Run_PartialPriceFailure_StillCompletes(t *testing.T) {
	wallets := []domain.Wallet{
		{ID: "w1", Blockchain: "ethereum", Address: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").Return(wallets, nil)

	snapshotRepo := &mocks.SnapshotRepository{}
	snapshotRepo.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("CreateSnapshotItem", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("UpdateSnapshotStatus", mock.Anything, mock.Anything, "completed").Return(nil)

	ethProvider := &mocks.BalanceProvider{}
	ethProvider.On("GetBalance", mock.Anything, mock.Anything).Return(1.23, "ETH", nil)

	priceProvider := &mocks.PriceProvider{}
	priceProvider.On("GetPriceUSD", mock.Anything, "ETH").
		Return(0.0, errors.New("coingecko down"))

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 1)

	result, err := runner.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.Len(t, result.Items, 1)
	require.Equal(t, 1.23, result.Items[0].Amount)
	require.Equal(t, 0.0, result.Items[0].USDPrice)
	require.Equal(t, 0.0, result.Items[0].USDValue)
}

// TestRunner_Run_ContextCancellation covers the interesting concurrency path:
// once the parent context is cancelled, workers should stop issuing RPC calls.
func TestRunner_Run_ContextCancellation(t *testing.T) {
	// 10 wallets; we'll cancel partway through.
	wallets := make([]domain.Wallet, 10)
	for i := range wallets {
		wallets[i] = domain.Wallet{
			ID: "w" + string(rune('a'+i)),
			Blockchain: "ethereum",
			Address: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		}
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").Return(wallets, nil)

	snapshotRepo := &mocks.SnapshotRepository{}
	snapshotRepo.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("CreateSnapshotItem", mock.Anything, mock.Anything).Return(nil).Maybe()
	snapshotRepo.On("UpdateSnapshotStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	var balanceCalls atomic.Int32
	ethProvider := &mocks.BalanceProvider{}
	ethProvider.
		On("GetBalance", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			balanceCalls.Add(1)
			// Simulate a slow RPC so we can cancel mid-flight.
			select {
			case <-args.Get(0).(context.Context).Done():
			case <-time.After(100 * time.Millisecond):
			}
		}).
		Return(1.0, "ETH", nil)

	priceProvider := &mocks.PriceProvider{}
	priceProvider.On("GetPriceUSD", mock.Anything, "ETH").Return(3000.0, nil).Maybe()

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 2)

	ctx, cancel := context.WithCancel(t.Context())
	// Cancel quickly, before all 10 wallets are drained by a 2-worker pool.
	time.AfterFunc(150*time.Millisecond, cancel)

	_, _ = runner.Run(ctx)

	// Not all wallets should have been fetched.
	require.Less(t, int(balanceCalls.Load()), len(wallets),
		"expected cancellation to stop the pipeline before all wallets were processed")
}

// =============================================================================
// Benchmark — snapshot pipeline with in-memory mocks.
//
// Useful to students exploring pprof:
//   go test ./internal/snapshot -bench=. -benchmem -run=^$ \
//     -cpuprofile=cpu.out && go tool pprof cpu.out
// =============================================================================

func BenchmarkRunner_Run_50Wallets(b *testing.B) {
	wallets := make([]domain.Wallet, 50)
	for i := range wallets {
		wallets[i] = domain.Wallet{
			ID: "w", Blockchain: "ethereum",
			Address: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		}
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").Return(wallets, nil)

	snapshotRepo := &mocks.SnapshotRepository{}
	snapshotRepo.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("CreateSnapshotItem", mock.Anything, mock.Anything).Return(nil)
	snapshotRepo.On("UpdateSnapshotStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	ethProvider := &mocks.BalanceProvider{}
	ethProvider.On("GetBalance", mock.Anything, mock.Anything).Return(1.0, "ETH", nil)

	priceProvider := &mocks.PriceProvider{}
	priceProvider.On("GetPriceUSD", mock.Anything, "ETH").Return(3000.0, nil)

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 8)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = runner.Run(b.Context())
	}
}
