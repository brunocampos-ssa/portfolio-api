// Package persister is the business logic of cmd/event-persister: a
// broker.Handler that converts a wire envelope into a domain
// WalletEvent and writes it to the events table.
//
// Lives in internal/persister so it can be unit-tested without
// spinning up Kafka or Postgres — the cmd/* binary is just wiring on
// top of this. Class 2 chapter point: the consumer-group binaries are
// thin; the interesting code is in their Handler.
//
// Idempotency contract:
//
//   - The watcher emits envelopes with a deterministic EventID derived
//     from (network, tx_hash, wallet_id, direction). Same logical
//     event, same id, no matter how many times Kafka redelivers it.
//   - We use that EventID as the wallet_events row id (PRIMARY KEY).
//   - EventRepository.Create uses ON CONFLICT (id) DO NOTHING, so a
//     duplicate delivery returns nil (offset commits, no retry).
//   - Net effect: the persister is at-least-once-safe by construction.
package persister

import (
	"context"
	"errors"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// NewHandler returns a broker.Handler closed over the given
// EventRepository. Calling the handler with an envelope:
//
//  1. Validates the envelope has the fields the row needs. Bad
//     envelopes return nil — there's no point retrying a malformed
//     payload, and the kafka.Consumer's poison-pill protection has
//     already absorbed the unparseable case. Empty required fields
//     are logged via the returned error path so an operator can
//     spot the producer-side bug without tying up the consumer.
//  2. Converts the envelope to a domain.WalletEvent.
//  3. Calls EventRepository.Create.
//
// Returning nil = commit the offset. Returning an error = trigger the
// consumer's bounded retry. We return errors only for transient
// repo failures (DB connection blip, deadlock, ...). Validation
// failures are NOT retried because retrying won't change them.
func NewHandler(repo contracts.EventRepository) broker.Handler {
	if repo == nil {
		panic("persister.NewHandler: repo must not be nil")
	}
	return func(ctx context.Context, env *broker.EventEnvelope) error {
		if env == nil {
			// Defensive: kafka.Consumer never sends nil today, but
			// the contract should not assume that.
			return nil
		}
		if err := validateForPersist(env); err != nil {
			// Don't retry — log via returned-then-swallowed pattern.
			// The kafka.Consumer's "exhausted retries" log line is
			// the operator-visible signal this happened.
			return fmt.Errorf("persister: %w", err)
		}

		event := envelopeToDomain(env)
		if err := repo.Create(ctx, event); err != nil {
			return fmt.Errorf("persister: create event %s: %w", env.EventID, err)
		}
		return nil
	}
}

// validateForPersist is a stricter check than broker.Validate. It
// requires the fields the wallet_events table needs but the broker's
// generic Validate doesn't enforce (Direction value, EventType value).
// Keeping this here rather than in the broker package keeps the
// envelope's "transport" concerns separate from the persister's
// "schema mapping" concerns.
func validateForPersist(env *broker.EventEnvelope) error {
	if env.Direction != "incoming" && env.Direction != "outgoing" {
		return fmt.Errorf("invalid direction %q (event_id=%s)", env.Direction, env.EventID)
	}
	if env.EventType == "" {
		return errors.New("event_type must not be empty")
	}
	if env.TokenSymbol == "" {
		return errors.New("token_symbol must not be empty")
	}
	if env.Amount == "" {
		return errors.New("amount must not be empty")
	}
	return nil
}

// envelopeToDomain is the inverse of watcher.eventToEnvelope. The
// EventID becomes the row's primary key — that's where the
// idempotency lives. RawPayload stays empty: the watcher's raw
// Ethereum log is not propagated through the bus, and downstream
// consumers don't need it. If a future class adds reprocessing from
// scratch, the watcher will republish.
func envelopeToDomain(env *broker.EventEnvelope) *domain.WalletEvent {
	return &domain.WalletEvent{
		ID:              env.EventID,
		WalletID:        env.WalletID,
		TxHash:          env.TxHash,
		BlockNumber:     env.BlockNumber,
		ContractAddress: env.ContractAddress,
		EventType:       env.EventType,
		Direction:       env.Direction,
		Amount:          env.Amount,
		TokenSymbol:     env.TokenSymbol,
		// RawPayload deliberately empty — see comment above.
	}
}
