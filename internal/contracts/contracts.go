package contracts

import (
	"context"
	"encoding/json"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Repository Interfaces
// =============================================================================

// UserRepository defines how user data is loaded from persistence.
type UserRepository interface {
	FindByID(ctx context.Context, id string) (*domain.User, error)
}

// WalletRepository defines how wallet data is loaded from persistence.
type WalletRepository interface {
	FindByUserID(ctx context.Context, userID string) ([]domain.Wallet, error)
	FindByBlockchain(ctx context.Context, blockchain string) ([]domain.Wallet, error)
	Create(ctx context.Context, wallet *domain.Wallet) error
}

// EventRepository persists blockchain events detected by the watcher.
type EventRepository interface {
	Create(ctx context.Context, event *domain.WalletEvent) error
}

// SnapshotRepository persists wallet snapshots and their items.
type SnapshotRepository interface {
	CreateSnapshot(ctx context.Context, snapshot *domain.WalletSnapshot) error
	CreateSnapshotItem(ctx context.Context, item *domain.WalletSnapshotItem) error
	UpdateSnapshotStatus(ctx context.Context, id, status string) error
}

// =============================================================================
// Provider Interfaces
// =============================================================================

// BalanceProvider fetches the on-chain balance for a wallet.
// Each blockchain network has its own implementation (Ethereum, Klever, etc.),
// but the service layer only depends on this interface — polymorphism in action.
type BalanceProvider interface {
	// GetBalance returns the native asset balance for the given wallet address.
	GetBalance(ctx context.Context, walletAddress string) (balance float64, asset string, err error)

	// Network returns the blockchain network this provider supports.
	Network() string
}

// PriceProvider fetches the current USD price for a given crypto asset.
type PriceProvider interface {
	GetPriceUSD(ctx context.Context, asset string) (float64, error)
}

// LogsFetcher fetches event logs from an Ethereum node.
// Used by the event watcher to poll for new ERC-20 Transfer events.
type LogsFetcher interface {
	// FetchLogs retrieves logs matching the given addresses starting from fromBlock.
	// Returns raw JSON log entries and the latest block number seen.
	FetchLogs(ctx context.Context, addresses []string, fromBlock uint64) (logs []json.RawMessage, lastBlock uint64, err error)
}

// TokenBalanceProvider fetches ERC-20 token balances for a wallet.
type TokenBalanceProvider interface {
	// GetTokenBalance returns the token balance for a wallet at a specific contract.
	GetTokenBalance(ctx context.Context, walletAddress, contractAddress string, decimals int) (float64, error)
}
