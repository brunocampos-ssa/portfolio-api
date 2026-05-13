package domain

import "time"

// WalletEvent represents a blockchain event (e.g., ERC-20 Transfer)
// associated with a tracked wallet.
//
// Events are detected by the event-watcher and persisted for auditing
// and future tax reporting purposes.
type WalletEvent struct {
	ID              string `json:"id" db:"id"`
	WalletID        string `json:"wallet_id" db:"wallet_id"`
	TxHash          string `json:"tx_hash" db:"tx_hash"`
	// LogIndex is the position of this log within its transaction,
	// as returned by eth_getLogs (hex-encoded, e.g. "0x0"). Used by
	// the watcher's makeEventID to keep EventIDs unique when a
	// single tx emits multiple Transfer logs hitting the same
	// wallet/direction. Not persisted as a column today (the EventID
	// already encodes it) but kept on the in-memory entity so the
	// envelope-builder has access to it.
	LogIndex        string    `json:"log_index"`
	BlockNumber     uint64    `json:"block_number" db:"block_number"`
	ContractAddress string    `json:"contract_address" db:"contract_address"`
	EventType       string    `json:"event_type" db:"event_type"` // e.g. "transfer"
	Direction       string    `json:"direction" db:"direction"`   // "incoming" or "outgoing"
	Amount          string    `json:"amount" db:"amount"`         // string to preserve precision
	TokenSymbol     string    `json:"token_symbol" db:"token_symbol"`
	RawPayload      string    `json:"raw_payload" db:"raw_payload"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
}
