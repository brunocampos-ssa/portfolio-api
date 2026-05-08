// Package broker is Class 2's cross-process event bus abstraction.
//
// Class 1's event-watcher fanned out to in-process consumers via Go
// channels. Class 2 replaces the in-process FanOut with a Kafka-as-
// source-of-truth model, plus a router service that mirrors selected
// events into a RabbitMQ topic exchange. The two transports teach
// different primitives:
//
//   - Kafka: durable partitioned log, consumer groups with independent
//     offsets, replay by seeking, partition keys for ordering. Used by
//     event-persister, event-router, and event-analytics — three groups,
//     three independent views of the same stream.
//
//   - RabbitMQ: topic exchange with routing keys (network.direction.token),
//     queue-per-consumer with ack/nack and DLQ. Used by event-notifier
//     to subscribe to a filtered slice ("any incoming USDC", "any
//     ethereum.outgoing.*", etc.).
//
// The architecture for Class 2:
//
//	[poller] → [normalize] → [Kafka publisher] → [Kafka topic]
//	                                                     │
//	         ┌───────────────────────────────────────────┼─────────────────────────┐
//	         ▼                                           ▼                         ▼
//	┌──────────────────┐                       ┌──────────────────┐      ┌──────────────────┐
//	│ event-persister  │                       │  event-router    │      │ event-analytics  │
//	│ (CG: persister)  │                       │ (CG: router)     │      │ (CG: analytics)  │
//	└────────┬─────────┘                       └────────┬─────────┘      └────────┬─────────┘
//	         │ Postgres                                 │                         │ in-mem agg
//	         ▼                                          ▼                         ▼
//	    wallet_events                          ┌──────────────────┐
//	                                           │ RabbitMQ exchange│
//	                                           │ wallet.events    │
//	                                           │ (topic)          │
//	                                           └────────┬─────────┘
//	                                                    │ routing key
//	                                                    ▼
//	                                           ┌──────────────────┐
//	                                           │ event-notifier   │
//	                                           │ (queue, *.in.*)  │
//	                                           └──────────────────┘
//
// The envelope below is the wire format on Kafka. We start with JSON for
// readability — students can `kafkacat`/`kcat` the topic and see legible
// payloads. Promotion to protobuf is a small swap once the chapter
// covers schema evolution.
package broker

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventEnvelope is the wire format used on the Kafka topic and re-emitted
// by the router into RabbitMQ. It is a transport concern: business logic
// works with domain.WalletEvent and converts at the boundaries.
//
// Fields are JSON-tagged with snake_case to match the rest of the API's
// external-facing serialisation conventions.
type EventEnvelope struct {
	// EventID is a globally-unique identifier for this delivery. Distinct
	// from the on-chain transaction hash because a single tx can produce
	// multiple wallet events (e.g., an internal transfer that hits two
	// tracked wallets in one log) and we still want each to be uniquely
	// traceable across the bus.
	EventID string `json:"event_id"`

	// SchemaVersion lets consumers reject envelopes from an incompatible
	// future schema. Bump on breaking changes; additive changes leave it
	// alone. v1 = the initial Class 2 wire shape.
	SchemaVersion int `json:"schema_version"`

	// Network is the chain identifier ("ethereum", "klever", ...).
	// Doubles as the first segment of the RabbitMQ routing key.
	Network string `json:"network"`

	// EventType is "transfer" today. Reserved for future
	// "approval", "swap", etc. Second segment of the routing key.
	EventType string `json:"event_type"`

	// Direction is "incoming" or "outgoing" relative to the tracked
	// wallet. Producers MUST normalise this — consumers depend on it for
	// routing-key matching.
	Direction string `json:"direction"`

	// WalletID is the database id of the tracked wallet this event hit.
	WalletID string `json:"wallet_id"`

	// TokenSymbol is the ERC-20 symbol or chain-native asset ("USDC",
	// "ETH", ...). Third segment of the routing key (lowercased).
	TokenSymbol string `json:"token_symbol"`

	// ContractAddress is the on-chain contract this event came from
	// (lowercase). For ERC-20 transfers this disambiguates same-symbol
	// tokens across chains (USDC on mainnet vs L2). The persister
	// stores it directly; downstream analytics can group by it.
	ContractAddress string `json:"contract_address"`

	// Amount is the human-readable decimal amount as a string to avoid
	// the float-precision trap. Consumers parse with shopspring/decimal
	// or math/big as appropriate.
	Amount string `json:"amount"`

	// TxHash is the on-chain transaction hash. The persister uses it as
	// the idempotency key — replays are safe because the wallet_events
	// table has a UNIQUE constraint on (tx_hash, wallet_id, direction).
	TxHash string `json:"tx_hash"`

	// BlockNumber is the chain height the event was observed at. Useful
	// for ordering and for the analytics consumer that windows by block.
	BlockNumber uint64 `json:"block_number"`

	// EmittedAt is when the watcher published the envelope (NOT the
	// block timestamp). Used for end-to-end latency metrics and for
	// the audit log.
	EmittedAt time.Time `json:"emitted_at"`
}

// Marshal returns the canonical JSON encoding of the envelope. Centralised
// so the publisher and the router emit byte-identical bytes regardless of
// build tag, struct field order changes, or future codec swaps.
func (e *EventEnvelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("broker: marshal envelope: %w", err)
	}
	return json.Marshal(e)
}

// Unmarshal parses bytes from Kafka or RabbitMQ into an envelope and
// validates it. Returns ErrSchemaMismatch when SchemaVersion is unknown
// so consumers can route the message to a DLQ rather than crash.
func Unmarshal(data []byte) (*EventEnvelope, error) {
	var e EventEnvelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("broker: unmarshal envelope: %w", err)
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

// SchemaCurrent is the only schema version this build understands.
// Bump when introducing breaking changes; additive changes do not bump.
const SchemaCurrent = 1

// ErrSchemaMismatch surfaces when an envelope is parseable but its
// SchemaVersion is not what this build supports. Consumers MUST handle
// this — typically by routing to a DLQ and emitting a metric. Crashing
// is wrong: a future producer rolling out ahead of consumers must not
// take down the fleet.
var ErrSchemaMismatch = fmt.Errorf("broker: envelope schema version mismatch")

// Validate enforces the minimum invariants every envelope must satisfy
// to be safe to route. Producers SHOULD call Marshal (which calls this);
// consumers SHOULD call Unmarshal (which also calls this).
func (e *EventEnvelope) Validate() error {
	if e.SchemaVersion != SchemaCurrent {
		return fmt.Errorf("%w: got %d, want %d", ErrSchemaMismatch, e.SchemaVersion, SchemaCurrent)
	}
	if e.EventID == "" {
		return fmt.Errorf("broker: envelope missing event_id")
	}
	if e.Network == "" {
		return fmt.Errorf("broker: envelope missing network")
	}
	if e.WalletID == "" {
		return fmt.Errorf("broker: envelope missing wallet_id")
	}
	if e.TxHash == "" {
		return fmt.Errorf("broker: envelope missing tx_hash")
	}
	if e.ContractAddress == "" {
		return fmt.Errorf("broker: envelope missing contract_address")
	}
	return nil
}
