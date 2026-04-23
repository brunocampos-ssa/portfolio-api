package watcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/mocks"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Watcher behavioural tests
//
// The watcher has an ownership model that is worth protecting with tests:
//
//   - the poller owns the raw-logs channel and must close it
//   - the normalizer (pipeline.Stage) owns the normalized channel
//   - the fan-out owns the consumer channels
//   - context cancellation must reach all goroutines
//
// These tests feed a scripted LogsFetcher, verify events land in the mock
// event repository, and assert that the watcher exits cleanly when its
// context is cancelled. They double as concurrency tests under `-race`.
// =============================================================================

// scriptedLogsFetcher is a hand-rolled fetcher so we can coordinate when the
// watcher first sees data (important for tests that care about ordering).
// testify/mock works great for "did we call FetchLogs?" assertions, but here
// we also need to hand the watcher a pre-computed raw log blob, so the handy
// channel-based trigger lives in this helper.
type scriptedLogsFetcher struct {
	mu       sync.Mutex
	trigger  chan struct{} // closed when the test has pushed a batch
	batch    []json.RawMessage
	lastCall atomic.Int32
}

func newScriptedLogsFetcher() *scriptedLogsFetcher {
	return &scriptedLogsFetcher{trigger: make(chan struct{})}
}

// push sets the next batch for the fetcher and releases any waiting poller.
func (s *scriptedLogsFetcher) push(batch []json.RawMessage) {
	s.mu.Lock()
	s.batch = batch
	s.mu.Unlock()
	close(s.trigger)
}

func (s *scriptedLogsFetcher) FetchLogs(ctx context.Context, _ []string, _ uint64) ([]json.RawMessage, uint64, error) {
	s.lastCall.Add(1)

	// Block the first call until the test pushes data. Subsequent calls return
	// nothing so we don't replay the same batch endlessly.
	select {
	case <-s.trigger:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}

	s.mu.Lock()
	batch := s.batch
	s.batch = nil
	s.mu.Unlock()
	return batch, uint64(len(batch)), nil
}

func buildTransferLog(t *testing.T, addr, from, to, blockHex string) json.RawMessage {
	t.Helper()
	l := map[string]any{
		"transactionHash": "0xabc",
		"blockNumber":     blockHex,
		"address":         addr,
		"topics": []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			from,
			to,
		},
		"data":    "0x0000000000000000000000000000000000000000000000000000000005f5e100",
		"removed": false,
	}
	data, err := json.Marshal(l)
	require.NoError(t, err)
	return data
}

// TestWatcher_Run_NoTrackedWallets_ExitsOnCtx verifies that when no ETH
// wallets exist, the watcher waits for shutdown rather than panicking or
// busy-looping.
func TestWatcher_Run_NoTrackedWallets_ExitsOnCtx(t *testing.T) {
	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").
		Return([]domain.Wallet{}, nil)

	logsFetcher := newScriptedLogsFetcher() // never triggered — intentional
	eventRepo := &mocks.EventRepository{}

	w := watcher.NewWatcher(logsFetcher, walletRepo, eventRepo, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	err := w.Run(ctx)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
	walletRepo.AssertExpectations(t)
}

// TestWatcher_Run_PersistsMatchedEvent drives a tracked wallet through the
// full pipeline (poller → normalizer → fan-out → persist worker) and
// verifies EventRepository.Create was invoked.
func TestWatcher_Run_PersistsMatchedEvent(t *testing.T) {
	wallet := domain.Wallet{
		ID:         "w1",
		UserID:     "u1",
		Blockchain: "ethereum",
		Address:    "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").
		Return([]domain.Wallet{wallet}, nil)

	logsFetcher := newScriptedLogsFetcher()
	eventRepo := &mocks.EventRepository{}

	// Capture the first persisted event so we can assert on its fields.
	persisted := make(chan *domain.WalletEvent, 1)
	eventRepo.
		On("Create", mock.Anything, mock.AnythingOfType("*domain.WalletEvent")).
		Run(func(args mock.Arguments) {
			ev := args.Get(1).(*domain.WalletEvent)
			select {
			case persisted <- ev:
			default:
			}
		}).
		Return(nil)

	w := watcher.NewWatcher(logsFetcher, walletRepo, eventRepo, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- w.Run(ctx) }()

	// Push one matching Transfer event (to=wallet) → expect one persisted event.
	logsFetcher.push([]json.RawMessage{
		buildTransferLog(t,
			"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
			"0xa",
		),
	})

	select {
	case ev := <-persisted:
		assert.Equal(t, "w1", ev.WalletID)
		assert.Equal(t, "incoming", ev.Direction)
		assert.Equal(t, "USDC", ev.TokenSymbol)
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("no event persisted within deadline")
	}

	cancel()
	err := <-runErr
	require.True(t, errors.Is(err, context.Canceled))
}

// TestWatcher_Run_FetcherError_KeepsRunning verifies the watcher is resilient
// to transient upstream failures: the poller logs them but continues polling
// rather than tearing the whole pipeline down.
func TestWatcher_Run_FetcherError_KeepsRunning(t *testing.T) {
	wallet := domain.Wallet{
		ID: "w1", Blockchain: "ethereum",
		Address: "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
	}

	walletRepo := &mocks.WalletRepository{}
	walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").
		Return([]domain.Wallet{wallet}, nil)

	var calls atomic.Int32
	logsFetcher := &failingLogsFetcher{calls: &calls}

	eventRepo := &mocks.EventRepository{}

	w := watcher.NewWatcher(logsFetcher, walletRepo, eventRepo, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	err := w.Run(ctx)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))

	// The poller should have retried several times despite FetchLogs returning
	// errors. Exact count depends on timing — assert at least 2.
	require.GreaterOrEqual(t, int(calls.Load()), 2, "poller should retry after transient errors")
}

// TestNewWatcher_PanicsOnNilDeps mirrors the pattern used by snapshot.Runner.
// Defensive constructors catch wiring mistakes at startup.
func TestNewWatcher_PanicsOnNilDeps(t *testing.T) {
	goodLogs := &mocks.LogsFetcher{}
	goodWallets := &mocks.WalletRepository{}
	goodEvents := &mocks.EventRepository{}

	cases := []struct {
		name string
		fn   func()
	}{
		{"nil logsFetcher", func() { watcher.NewWatcher(nil, goodWallets, goodEvents, time.Second) }},
		{"nil walletRepo", func() { watcher.NewWatcher(goodLogs, nil, goodEvents, time.Second) }},
		{"nil eventRepo", func() { watcher.NewWatcher(goodLogs, goodWallets, nil, time.Second) }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Panics(t, c.fn)
		})
	}
}

// failingLogsFetcher always errors — tests the watcher's retry behaviour.
type failingLogsFetcher struct {
	calls *atomic.Int32
}

func (f *failingLogsFetcher) FetchLogs(ctx context.Context, _ []string, _ uint64) ([]json.RawMessage, uint64, error) {
	f.calls.Add(1)
	return nil, 0, errors.New("transient upstream error")
}
