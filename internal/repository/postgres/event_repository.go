package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// EventRepository implements contracts.EventRepository using PostgreSQL.
type EventRepository struct {
	db *sql.DB
}

// NewEventRepository creates a new PostgreSQL-backed event repository.
func NewEventRepository(db *sql.DB) *EventRepository {
	if db == nil {
		panic("postgres.NewEventRepository: db must not be nil")
	}
	return &EventRepository{db: db}
}

// Create inserts a new wallet event into the database.
func (r *EventRepository) Create(ctx context.Context, event *domain.WalletEvent) error {
	const op = "EventRepository.Create"

	query := `INSERT INTO wallet_events (id, wallet_id, tx_hash, block_number, contract_address, event_type, direction, amount, token_symbol, raw_payload, created_at)
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		event.ID, event.WalletID, event.TxHash, event.BlockNumber,
		event.ContractAddress, event.EventType, event.Direction,
		event.Amount, event.TokenSymbol, event.RawPayload,
	).Scan(&event.CreatedAt)
	if err != nil {
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}

	return nil
}
