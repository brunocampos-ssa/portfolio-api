package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// SnapshotRepository implements contracts.SnapshotRepository using PostgreSQL.
type SnapshotRepository struct {
	db *sql.DB
}

// NewSnapshotRepository creates a new PostgreSQL-backed snapshot repository.
func NewSnapshotRepository(db *sql.DB) *SnapshotRepository {
	if db == nil {
		panic("postgres.NewSnapshotRepository: db must not be nil")
	}
	return &SnapshotRepository{db: db}
}

// CreateSnapshot inserts a new wallet snapshot record.
func (r *SnapshotRepository) CreateSnapshot(ctx context.Context, snapshot *domain.WalletSnapshot) error {
	const op = "SnapshotRepository.CreateSnapshot"

	query := `INSERT INTO wallet_snapshots (id, reference_time, status, created_at)
	          VALUES ($1, $2, $3, NOW())
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		snapshot.ID, snapshot.ReferenceTime, snapshot.Status,
	).Scan(&snapshot.CreatedAt)
	if err != nil {
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}

	return nil
}

// CreateSnapshotItem inserts a single snapshot item (one asset in one wallet).
func (r *SnapshotRepository) CreateSnapshotItem(ctx context.Context, item *domain.WalletSnapshotItem) error {
	const op = "SnapshotRepository.CreateSnapshotItem"

	query := `INSERT INTO wallet_snapshot_items (id, snapshot_id, wallet_id, asset_symbol, asset_type, contract_address, amount, usd_price, usd_value, created_at)
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		item.ID, item.SnapshotID, item.WalletID, item.AssetSymbol,
		item.AssetType, item.ContractAddress, item.Amount,
		item.USDPrice, item.USDValue,
	).Scan(&item.CreatedAt)
	if err != nil {
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}

	return nil
}

// UpdateSnapshotStatus updates the status of a snapshot (e.g., "completed", "failed").
func (r *SnapshotRepository) UpdateSnapshotStatus(ctx context.Context, id, status string) error {
	const op = "SnapshotRepository.UpdateSnapshotStatus"

	query := `UPDATE wallet_snapshots SET status = $2 WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query, id, status)
	if err != nil {
		return fmt.Errorf("%s: update failed: %w", op, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: rows affected: %w", op, err)
	}
	if rows == 0 {
		return fmt.Errorf("%s: snapshot %s not found", op, id)
	}

	return nil
}
