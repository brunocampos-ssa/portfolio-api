package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// =============================================================================
// Mock implementations
// =============================================================================

type mockUserRepo struct {
	users map[string]*domain.User
}

func (m *mockUserRepo) FindByID(_ context.Context, id string) (*domain.User, error) {
	user, ok := m.users[id]
	if !ok {
		// Simulate what the real repo does: wrap ErrUserNotFound
		return nil, fmt.Errorf("UserRepository.FindByID: %w", domain.ErrUserNotFound)
	}
	return user, nil
}

// PortfolioService never calls FindByEmail/Create — these stubs exist only
// to satisfy the contracts.UserRepository interface added in Module 4.
func (m *mockUserRepo) FindByEmail(_ context.Context, _ string) (*domain.User, error) {
	return nil, fmt.Errorf("UserRepository.FindByEmail: %w", domain.ErrUserNotFound)
}

func (m *mockUserRepo) Create(_ context.Context, _ *domain.User, _ string) error {
	return errors.New("not implemented in this mock")
}

type mockWalletRepo struct {
	wallets map[string][]domain.Wallet
	err     error
}

func (m *mockWalletRepo) FindByUserID(_ context.Context, userID string) ([]domain.Wallet, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.wallets[userID], nil
}

func (m *mockWalletRepo) FindByBlockchain(_ context.Context, blockchain string) ([]domain.Wallet, error) {
	return nil, nil
}

func (m *mockWalletRepo) Create(_ context.Context, w *domain.Wallet) error {
	return nil
}

type mockBalanceProvider struct {
	network string
	balance float64
	asset   string
	err     error
}

func (m *mockBalanceProvider) Network() string { return m.network }

func (m *mockBalanceProvider) GetBalance(_ context.Context, _ string) (float64, string, error) {
	if m.err != nil {
		return 0, m.asset, m.err
	}
	return m.balance, m.asset, nil
}

type mockPriceProvider struct {
	prices map[string]float64
	err    error
}

func (m *mockPriceProvider) GetPriceUSD(_ context.Context, asset string) (float64, error) {
	if m.err != nil {
		return 0, m.err
	}
	price, ok := m.prices[asset]
	if !ok {
		return 0, fmt.Errorf("MockPriceProvider: %w: %s", domain.ErrUnsupportedAsset, asset)
	}
	return price, nil
}

// =============================================================================
// Helper to build service with defaults
// =============================================================================

func newTestService(
	userRepo *mockUserRepo,
	walletRepo *mockWalletRepo,
	providers []*mockBalanceProvider,
	priceProvider *mockPriceProvider,
) *service.PortfolioService {
	registry := blockchain.NewProviderRegistry()
	for _, p := range providers {
		registry.Register(p)
	}
	return service.NewPortfolioService(userRepo, walletRepo, registry, priceProvider)
}

// =============================================================================
// Test: Successful Portfolio
// =============================================================================

func TestGetPortfolio_Success(t *testing.T) {
	svc := newTestService(
		&mockUserRepo{users: map[string]*domain.User{
			"u1": {ID: "u1", Name: "Alice"},
		}},
		&mockWalletRepo{wallets: map[string][]domain.Wallet{
			"u1": {{ID: "w1", UserID: "u1", Blockchain: "ethereum", Address: "0xABC"}},
		}},
		[]*mockBalanceProvider{
			{network: "ethereum", balance: 2.5, asset: "ETH"},
		},
		&mockPriceProvider{prices: map[string]float64{"ETH": 4000.0}},
	)

	portfolio, err := svc.GetPortfolio(context.Background(), "u1")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if portfolio.UserID != "u1" {
		t.Errorf("UserID = %q, want %q", portfolio.UserID, "u1")
	}
	if len(portfolio.Holdings) != 1 {
		t.Fatalf("Holdings count = %d, want 1", len(portfolio.Holdings))
	}
	if portfolio.Holdings[0].Balance != 2.5 {
		t.Errorf("Balance = %f, want 2.5", portfolio.Holdings[0].Balance)
	}
	if portfolio.TotalUSD != 10000.0 {
		t.Errorf("TotalUSD = %f, want 10000.0", portfolio.TotalUSD)
	}
}

// =============================================================================
// Test: User Not Found → Domain Error Translation
// =============================================================================
//
// This is a KEY test for the lesson:
// The repository returns a wrapped ErrUserNotFound, the service translates
// it into an AppError with CodeNotFound, and errors.Is still works.

func TestGetPortfolio_UserNotFound(t *testing.T) {
	svc := newTestService(
		&mockUserRepo{users: map[string]*domain.User{}}, // empty — user won't be found
		&mockWalletRepo{wallets: map[string][]domain.Wallet{}},
		nil,
		&mockPriceProvider{prices: map[string]float64{}},
	)

	_, err := svc.GetPortfolio(context.Background(), "unknown")
	if err == nil {
		t.Fatal("expected error for unknown user")
	}

	// errors.Is works through the AppError chain
	if !errors.Is(err, domain.ErrUserNotFound) {
		t.Errorf("expected errors.Is(err, ErrUserNotFound) = true, got false. err: %v", err)
	}

	// errors.As extracts the AppError
	var appErr *domain.AppError
	if !errors.As(err, &appErr) {
		t.Fatal("expected errors.As to extract AppError")
	}
	if appErr.Code != domain.CodeNotFound {
		t.Errorf("AppError.Code = %q, want %q", appErr.Code, domain.CodeNotFound)
	}
}

// =============================================================================
// Test: Partial Failure — Provider Error Doesn't Kill Portfolio
// =============================================================================

func TestGetPortfolio_PartialFailure(t *testing.T) {
	svc := newTestService(
		&mockUserRepo{users: map[string]*domain.User{
			"u1": {ID: "u1", Name: "Multi"},
		}},
		&mockWalletRepo{wallets: map[string][]domain.Wallet{
			"u1": {
				{ID: "w1", UserID: "u1", Blockchain: "ethereum", Address: "0xABC"},
				{ID: "w2", UserID: "u1", Blockchain: "klever", Address: "klv1xyz"},
			},
		}},
		[]*mockBalanceProvider{
			{network: "ethereum", balance: 1.0, asset: "ETH"},
			// klever provider returns an error
			{network: "klever", asset: "KLV", err: fmt.Errorf("KleverProvider: %w: connection refused", domain.ErrProviderUnavailable)},
		},
		&mockPriceProvider{prices: map[string]float64{"ETH": 3000.0}},
	)

	portfolio, err := svc.GetPortfolio(context.Background(), "u1")
	if err != nil {
		t.Fatalf("expected no error (partial failure), got: %v", err)
	}

	if len(portfolio.Holdings) != 2 {
		t.Fatalf("Holdings count = %d, want 2", len(portfolio.Holdings))
	}

	// First holding should succeed.
	if portfolio.Holdings[0].Error != "" {
		t.Errorf("Holdings[0] should have no error, got: %q", portfolio.Holdings[0].Error)
	}
	if portfolio.Holdings[0].ValueUSD != 3000.0 {
		t.Errorf("Holdings[0].ValueUSD = %f, want 3000.0", portfolio.Holdings[0].ValueUSD)
	}

	// Second holding should have an error message.
	if portfolio.Holdings[1].Error == "" {
		t.Error("Holdings[1] should have an error")
	}

	// Total should only include the successful holding.
	if portfolio.TotalUSD != 3000.0 {
		t.Errorf("TotalUSD = %f, want 3000.0 (partial)", portfolio.TotalUSD)
	}
}

// =============================================================================
// Test: Empty Wallets — Valid But Empty Portfolio
// =============================================================================

func TestGetPortfolio_NoWallets(t *testing.T) {
	svc := newTestService(
		&mockUserRepo{users: map[string]*domain.User{
			"u1": {ID: "u1", Name: "New User"},
		}},
		&mockWalletRepo{wallets: map[string][]domain.Wallet{}},
		nil,
		&mockPriceProvider{prices: map[string]float64{}},
	)

	portfolio, err := svc.GetPortfolio(context.Background(), "u1")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(portfolio.Holdings) != 0 {
		t.Errorf("Holdings count = %d, want 0", len(portfolio.Holdings))
	}
	if portfolio.TotalUSD != 0 {
		t.Errorf("TotalUSD = %f, want 0", portfolio.TotalUSD)
	}
}

// =============================================================================
// Test: Constructor Panics on Nil Dependencies
// =============================================================================
//
// Defensive programming: the constructor must refuse nil dependencies.
// We test this by catching the expected panic.

func TestNewPortfolioService_PanicsOnNilUserRepo(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil userRepo")
		}
	}()

	service.NewPortfolioService(nil, &mockWalletRepo{}, blockchain.NewProviderRegistry(), &mockPriceProvider{})
}

func TestNewPortfolioService_PanicsOnNilWalletRepo(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil walletRepo")
		}
	}()

	service.NewPortfolioService(&mockUserRepo{}, nil, blockchain.NewProviderRegistry(), &mockPriceProvider{})
}

func TestNewPortfolioService_PanicsOnNilPriceProvider(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil priceProvider")
		}
	}()

	service.NewPortfolioService(&mockUserRepo{}, &mockWalletRepo{}, blockchain.NewProviderRegistry(), nil)
}
