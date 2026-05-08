package postgres

import (
	"context"
	"database/sql"
	"errors"
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

// Create inserts a new wallet event idempotently.
//
// "Idempotently" because of the Class 2 messaging architecture: at-
// least-once Kafka redelivery is the norm, not the exception. The
// caller (event-persister) supplies a deterministic id derived from
// (network, tx_hash, wallet_id, direction) — same logical event, same
// id, regardless of how many times Kafka delivers it. ON CONFLICT (id)
// DO NOTHING absorbs duplicates without surfacing an error to the
// handler, which then returns nil and lets the consumer commit the
// offset.
//
// On a real conflict the RETURNING clause yields zero rows and Scan
// reports sql.ErrNoRows. We translate that into a successful no-op
// (event.CreatedAt stays at its zero value; callers that care can
// re-read the row).
func (r *EventRepository) Create(ctx context.Context, event *domain.WalletEvent) error {
	const op = "EventRepository.Create"

	query := `INSERT INTO wallet_events (id, wallet_id, tx_hash, block_number, contract_address, event_type, direction, amount, token_symbol, raw_payload, created_at)
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
	          ON CONFLICT (id) DO NOTHING
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		event.ID, event.WalletID, event.TxHash, event.BlockNumber,
		event.ContractAddress, event.EventType, event.Direction,
		event.Amount, event.TokenSymbol, event.RawPayload,
	).Scan(&event.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Conflict on id — the event is already persisted. The
			// handler treats this as success so the Kafka offset
			// advances and we don't loop on the same delivery.
			return nil
		}
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}

	return nil
}
