// Package mocks contains testify/mock-based fakes for the narrow external
// interfaces declared in internal/contracts. They exist so tests can assert
// against call expectations (argument matching, call count, ordering) without
// hand-rolling bespoke stubs in every test file.
//
// Guidelines:
//
//   - Prefer these mocks for tests that exercise error handling or call
//     orchestration — anything where "did we call X with Y?" matters.
//   - For simple table-driven tests that just need a function to return a
//     value, a plain struct with a field still reads better. Don't mock
//     reflexively.
//   - Mocks implement the interfaces declared in internal/contracts. If those
//     contracts grow, update the mocks here and tests will compile-fail
//     until the new method is handled.
package mocks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// Ensure mocks satisfy the interfaces at compile time.
var (
	_ contracts.UserRepository         = (*UserRepository)(nil)
	_ contracts.WalletRepository       = (*WalletRepository)(nil)
	_ contracts.EventRepository        = (*EventRepository)(nil)
	_ contracts.SnapshotRepository     = (*SnapshotRepository)(nil)
	_ contracts.RefreshTokenRepository = (*RefreshTokenRepository)(nil)
	_ contracts.BalanceProvider        = (*BalanceProvider)(nil)
	_ contracts.TokenBalanceProvider   = (*TokenBalanceProvider)(nil)
	_ contracts.PriceProvider          = (*PriceProvider)(nil)
	_ contracts.LogsFetcher            = (*LogsFetcher)(nil)
)

// -----------------------------------------------------------------------------
// Repositories
// -----------------------------------------------------------------------------

type UserRepository struct{ mock.Mock }

func (m *UserRepository) FindByID(ctx context.Context, id string) (*domain.User, error) {
	args := m.Called(ctx, id)
	u, _ := args.Get(0).(*domain.User)
	return u, args.Error(1)
}

func (m *UserRepository) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	args := m.Called(ctx, email)
	u, _ := args.Get(0).(*domain.User)
	return u, args.Error(1)
}

func (m *UserRepository) Create(ctx context.Context, user *domain.User, passwordHash string) error {
	return m.Called(ctx, user, passwordHash).Error(0)
}

type RefreshTokenRepository struct{ mock.Mock }

func (m *RefreshTokenRepository) Insert(ctx context.Context, t *domain.RefreshToken) error {
	return m.Called(ctx, t).Error(0)
}

func (m *RefreshTokenRepository) FindByHash(ctx context.Context, tokenHash string) (*domain.RefreshToken, error) {
	args := m.Called(ctx, tokenHash)
	t, _ := args.Get(0).(*domain.RefreshToken)
	return t, args.Error(1)
}

func (m *RefreshTokenRepository) MarkRotated(ctx context.Context, oldID, newID string, revokedAt time.Time) error {
	return m.Called(ctx, oldID, newID, revokedAt).Error(0)
}

func (m *RefreshTokenRepository) Revoke(ctx context.Context, id string, revokedAt time.Time) error {
	return m.Called(ctx, id, revokedAt).Error(0)
}

func (m *RefreshTokenRepository) RevokeFamily(ctx context.Context, startID string, revokedAt time.Time) error {
	return m.Called(ctx, startID, revokedAt).Error(0)
}

type WalletRepository struct{ mock.Mock }

func (m *WalletRepository) FindByUserID(ctx context.Context, userID string) ([]domain.Wallet, error) {
	args := m.Called(ctx, userID)
	w, _ := args.Get(0).([]domain.Wallet)
	return w, args.Error(1)
}

func (m *WalletRepository) FindByBlockchain(ctx context.Context, blockchain string) ([]domain.Wallet, error) {
	args := m.Called(ctx, blockchain)
	w, _ := args.Get(0).([]domain.Wallet)
	return w, args.Error(1)
}

func (m *WalletRepository) Create(ctx context.Context, w *domain.Wallet) error {
	return m.Called(ctx, w).Error(0)
}

type EventRepository struct{ mock.Mock }

func (m *EventRepository) Create(ctx context.Context, event *domain.WalletEvent) error {
	return m.Called(ctx, event).Error(0)
}

type SnapshotRepository struct{ mock.Mock }

func (m *SnapshotRepository) CreateSnapshot(ctx context.Context, s *domain.WalletSnapshot) error {
	return m.Called(ctx, s).Error(0)
}

func (m *SnapshotRepository) CreateSnapshotItem(ctx context.Context, item *domain.WalletSnapshotItem) error {
	return m.Called(ctx, item).Error(0)
}

func (m *SnapshotRepository) UpdateSnapshotStatus(ctx context.Context, id, status string) error {
	return m.Called(ctx, id, status).Error(0)
}

// -----------------------------------------------------------------------------
// Providers
// -----------------------------------------------------------------------------

type BalanceProvider struct{ mock.Mock }

func (m *BalanceProvider) GetBalance(ctx context.Context, walletAddress string) (float64, string, error) {
	args := m.Called(ctx, walletAddress)
	bal, _ := args.Get(0).(float64)
	asset, _ := args.Get(1).(string)
	return bal, asset, args.Error(2)
}

func (m *BalanceProvider) Network() string {
	return m.Called().String(0)
}

type TokenBalanceProvider struct{ mock.Mock }

func (m *TokenBalanceProvider) GetTokenBalance(ctx context.Context, wallet, contract string, decimals int) (float64, error) {
	args := m.Called(ctx, wallet, contract, decimals)
	bal, _ := args.Get(0).(float64)
	return bal, args.Error(1)
}

type PriceProvider struct{ mock.Mock }

func (m *PriceProvider) GetPriceUSD(ctx context.Context, asset string) (float64, error) {
	args := m.Called(ctx, asset)
	price, _ := args.Get(0).(float64)
	return price, args.Error(1)
}

type LogsFetcher struct{ mock.Mock }

func (m *LogsFetcher) FetchLogs(ctx context.Context, addresses []string, fromBlock uint64) ([]json.RawMessage, uint64, error) {
	args := m.Called(ctx, addresses, fromBlock)
	logs, _ := args.Get(0).([]json.RawMessage)
	block, _ := args.Get(1).(uint64)
	return logs, block, args.Error(2)
}
