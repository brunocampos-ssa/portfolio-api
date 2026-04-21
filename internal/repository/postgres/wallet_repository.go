package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// WalletRepository implements contracts.WalletRepository using PostgreSQL.
type WalletRepository struct {
	db *sql.DB
}

// NewWalletRepository creates a new PostgreSQL-backed wallet repository.
func NewWalletRepository(db *sql.DB) *WalletRepository {
	if db == nil {
		panic("postgres.NewWalletRepository: db must not be nil")
	}
	return &WalletRepository{db: db}
}

// FindByUserID returns all wallets for a given user.
//
// Note: an empty result is NOT an error. The user simply has no wallets yet.
// This is an important distinction — "no rows" here is a valid business state,
// unlike UserRepository.FindByID where "no rows" means the user doesn't exist.
func (r *WalletRepository) FindByUserID(ctx context.Context, userID string) ([]domain.Wallet, error) {
	const op = "WalletRepository.FindByUserID"

	query := `SELECT id, user_id, blockchain, address, created_at FROM wallets WHERE user_id = $1`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("%s: query failed: %w", op, err)
	}
	// defer ensures rows.Close() runs even if we return early due to an error
	// during scanning. Without this, a database connection could leak.
	defer rows.Close()

	var wallets []domain.Wallet
	for rows.Next() {
		var w domain.Wallet
		if err := rows.Scan(&w.ID, &w.UserID, &w.Blockchain, &w.Address, &w.CreatedAt); err != nil {
			return nil, fmt.Errorf("%s: scan failed: %w", op, err)
		}
		wallets = append(wallets, w)
	}

	// rows.Err() catches errors that occurred during iteration (e.g., network issues).
	// Many Go developers forget this check — it's a common source of silent bugs.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows iteration failed: %w", op, err)
	}

	return wallets, nil
}

// FindByBlockchain returns all wallets for a given blockchain network.
// Used by the event watcher to load all tracked Ethereum addresses.
func (r *WalletRepository) FindByBlockchain(ctx context.Context, blockchain string) ([]domain.Wallet, error) {
	const op = "WalletRepository.FindByBlockchain"

	query := `SELECT id, user_id, blockchain, address, created_at FROM wallets WHERE blockchain = $1`

	rows, err := r.db.QueryContext(ctx, query, blockchain)
	if err != nil {
		return nil, fmt.Errorf("%s: query failed: %w", op, err)
	}
	defer rows.Close()

	var wallets []domain.Wallet
	for rows.Next() {
		var w domain.Wallet
		if err := rows.Scan(&w.ID, &w.UserID, &w.Blockchain, &w.Address, &w.CreatedAt); err != nil {
			return nil, fmt.Errorf("%s: scan failed: %w", op, err)
		}
		wallets = append(wallets, w)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows iteration failed: %w", op, err)
	}

	return wallets, nil
}

// Create inserts a new wallet into the database.
func (r *WalletRepository) Create(ctx context.Context, wallet *domain.Wallet) error {
	const op = "WalletRepository.Create"

	query := `INSERT INTO wallets (id, user_id, blockchain, address, created_at)
	          VALUES ($1, $2, $3, $4, NOW())
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		wallet.ID, wallet.UserID, wallet.Blockchain, wallet.Address,
	).Scan(&wallet.CreatedAt)
	if err != nil {
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}

	return nil
}
