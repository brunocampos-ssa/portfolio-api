package contracts

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Repository Interfaces
// =============================================================================

// UserRepository defines how user data is loaded from persistence.
//
// Module 4 grew this interface from a single FindByID lookup into a real
// CRUD surface so the auth service can register users and authenticate
// them by email.
type UserRepository interface {
	FindByID(ctx context.Context, id string) (*domain.User, error)
	FindByEmail(ctx context.Context, email string) (*domain.User, error)
	Create(ctx context.Context, user *domain.User, passwordHash string) error
}

// RefreshTokenRepository persists issued refresh tokens. Tokens are stored
// only as their SHA-256 hash; the plaintext is shown to the client once
// at issuance and never recovered.
type RefreshTokenRepository interface {
	// Insert records a freshly issued refresh token.
	Insert(ctx context.Context, token *domain.RefreshToken) error

	// FindByHash loads a token by its SHA-256 hash. Returns
	// domain.ErrRefreshTokenNotFound when no row matches.
	FindByHash(ctx context.Context, tokenHash string) (*domain.RefreshToken, error)

	// MarkRotated atomically marks oldID as revoked and links it to newID
	// via replaced_by. Used during /auth/refresh to record the rotation.
	MarkRotated(ctx context.Context, oldID, newID string, revokedAt time.Time) error

	// Revoke marks a single token revoked. Idempotent — revoking an
	// already-revoked token is a no-op.
	Revoke(ctx context.Context, id string, revokedAt time.Time) error

	// RevokeFamily revokes the entire chain reachable via replaced_by from
	// the given starting token. Used when a replay is detected: revoking
	// the chain forces every device that holds any token in the lineage
	// to re-authenticate.
	RevokeFamily(ctx context.Context, startID string, revokedAt time.Time) error
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
