package snapshot_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/snapshot"
)

// =============================================================================
// Mock implementations for testing the snapshot runner
// =============================================================================

type mockWalletRepo struct {
	wallets []domain.Wallet
}

func (m *mockWalletRepo) FindByUserID(_ context.Context, _ string) ([]domain.Wallet, error) {
	return nil, nil
}

func (m *mockWalletRepo) FindByBlockchain(_ context.Context, blockchain string) ([]domain.Wallet, error) {
	if blockchain != "ethereum" {
		return nil, nil
	}
	return m.wallets, nil
}

func (m *mockWalletRepo) Create(_ context.Context, _ *domain.Wallet) error {
	return nil
}

type mockSnapshotRepo struct {
	snapshot *domain.WalletSnapshot
	items    []domain.WalletSnapshotItem
	status   string
}

func (m *mockSnapshotRepo) CreateSnapshot(_ context.Context, s *domain.WalletSnapshot) error {
	m.snapshot = s
	return nil
}

func (m *mockSnapshotRepo) CreateSnapshotItem(_ context.Context, item *domain.WalletSnapshotItem) error {
	m.items = append(m.items, *item)
	return nil
}

func (m *mockSnapshotRepo) UpdateSnapshotStatus(_ context.Context, id, status string) error {
	m.status = status
	return nil
}

type mockBalanceProvider struct {
	balances map[string]float64
}

func (m *mockBalanceProvider) GetBalance(_ context.Context, addr string) (float64, string, error) {
	balance, ok := m.balances[addr]
	if !ok {
		return 0, "ETH", fmt.Errorf("unknown address: %s", addr)
	}
	return balance, "ETH", nil
}

func (m *mockBalanceProvider) Network() string { return "ethereum" }

type mockTokenProvider struct {
	balances map[string]float64 // key: "wallet:contract"
}

func (m *mockTokenProvider) GetTokenBalance(_ context.Context, wallet, contract string, _ int) (float64, error) {
	key := wallet + ":" + contract
	balance, ok := m.balances[key]
	if !ok {
		return 0, nil
	}
	return balance, nil
}

type mockPriceProvider struct {
	prices map[string]float64
}

func (m *mockPriceProvider) GetPriceUSD(_ context.Context, asset string) (float64, error) {
	price, ok := m.prices[asset]
	if !ok {
		return 0, fmt.Errorf("unknown asset: %s", asset)
	}
	return price, nil
}

// =============================================================================
// Tests
// =============================================================================

// TestRunner_Run_ProcessesAllWallets verifies that the snapshot runner
// processes all wallets and produces snapshot items with correct values.
func TestRunner_Run_ProcessesAllWallets(t *testing.T) {
	walletRepo := &mockWalletRepo{
		wallets: []domain.Wallet{
			{ID: "w1", UserID: "u1", Blockchain: "ethereum", Address: "0xAAA"},
			{ID: "w2", UserID: "u2", Blockchain: "ethereum", Address: "0xBBB"},
		},
	}

	snapshotRepo := &mockSnapshotRepo{}

	ethProvider := &mockBalanceProvider{
		balances: map[string]float64{
			"0xAAA": 1.5,
			"0xBBB": 2.0,
		},
	}

	priceProvider := &mockPriceProvider{
		prices: map[string]float64{
			"ETH": 3000.0,
		},
	}

	runner := snapshot.NewRunner(
		walletRepo,
		snapshotRepo,
		ethProvider,
		nil, // no token provider
		priceProvider,
		2, // 2 workers
	)

	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Status != "completed" {
		t.Errorf("status = %q, want %q", result.Status, "completed")
	}

	// Should have 2 items (1 native ETH per wallet, no tokens).
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result.Items))
	}

	// Verify USD values are calculated correctly.
	totalUSD := 0.0
	for _, item := range result.Items {
		if item.AssetSymbol != "ETH" {
			t.Errorf("unexpected asset: %s", item.AssetSymbol)
		}
		if item.AssetType != "native" {
			t.Errorf("expected asset_type=native, got %s", item.AssetType)
		}
		totalUSD += item.USDValue
	}

	expectedTotal := (1.5 + 2.0) * 3000.0
	if totalUSD != expectedTotal {
		t.Errorf("total USD = %.2f, want %.2f", totalUSD, expectedTotal)
	}

	// Verify snapshot was persisted.
	if snapshotRepo.snapshot == nil {
		t.Error("snapshot was not persisted")
	}
	if snapshotRepo.status != "completed" {
		t.Errorf("persisted status = %q, want %q", snapshotRepo.status, "completed")
	}
}

// TestRunner_Run_WithTokens verifies that ERC-20 token balances
// are included when a token provider is available.
func TestRunner_Run_WithTokens(t *testing.T) {
	walletRepo := &mockWalletRepo{
		wallets: []domain.Wallet{
			{ID: "w1", UserID: "u1", Blockchain: "ethereum", Address: "0xAAA"},
		},
	}

	snapshotRepo := &mockSnapshotRepo{}

	ethProvider := &mockBalanceProvider{
		balances: map[string]float64{"0xAAA": 1.0},
	}

	tokenProvider := &mockTokenProvider{
		balances: map[string]float64{
			"0xAAA:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 500.0,  // USDC
			"0xAAA:0xdac17f958d2ee523a2206206994597c13d831ec7": 1000.0, // USDT
		},
	}

	priceProvider := &mockPriceProvider{
		prices: map[string]float64{"ETH": 3000.0},
	}

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, tokenProvider, priceProvider, 1)

	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 items: ETH + USDC + USDT.
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result.Items))
	}

	// Check that we have one native and two erc20 items.
	nativeCount := 0
	erc20Count := 0
	for _, item := range result.Items {
		switch item.AssetType {
		case "native":
			nativeCount++
		case "erc20":
			erc20Count++
		}
	}

	if nativeCount != 1 {
		t.Errorf("expected 1 native item, got %d", nativeCount)
	}
	if erc20Count != 2 {
		t.Errorf("expected 2 erc20 items, got %d", erc20Count)
	}
}

// TestRunner_Run_NoWallets verifies that the runner returns an error
// when there are no ethereum wallets to process.
func TestRunner_Run_NoWallets(t *testing.T) {
	walletRepo := &mockWalletRepo{wallets: nil}
	snapshotRepo := &mockSnapshotRepo{}
	ethProvider := &mockBalanceProvider{balances: map[string]float64{}}
	priceProvider := &mockPriceProvider{prices: map[string]float64{}}

	runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 2)

	_, err := runner.Run(context.Background())
	if err == nil {
		t.Error("expected error for no wallets")
	}
}

// TestNewRunner_PanicsOnNilDeps verifies that the constructor catches
// nil dependencies at startup (defensive programming).
func TestNewRunner_PanicsOnNilDeps(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "nil walletRepo",
			fn: func() {
				snapshot.NewRunner(nil, &mockSnapshotRepo{}, &mockBalanceProvider{}, nil, &mockPriceProvider{}, 2)
			},
		},
		{
			name: "nil snapshotRepo",
			fn: func() {
				snapshot.NewRunner(&mockWalletRepo{}, nil, &mockBalanceProvider{}, nil, &mockPriceProvider{}, 2)
			},
		},
		{
			name: "nil ethProvider",
			fn: func() {
				snapshot.NewRunner(&mockWalletRepo{}, &mockSnapshotRepo{}, nil, nil, &mockPriceProvider{}, 2)
			},
		},
		{
			name: "nil priceProvider",
			fn: func() {
				snapshot.NewRunner(&mockWalletRepo{}, &mockSnapshotRepo{}, &mockBalanceProvider{}, nil, nil, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic for nil dependency")
				}
			}()
			tt.fn()
		})
	}
}
