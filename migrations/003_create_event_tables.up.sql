-- Migration 003: Create wallet_events table for the event watcher.
-- Stores ERC-20 Transfer events detected for tracked wallets.

CREATE TABLE IF NOT EXISTS wallet_events (
    id               TEXT PRIMARY KEY,
    wallet_id        TEXT NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    tx_hash          TEXT NOT NULL,
    block_number     BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    event_type       TEXT NOT NULL DEFAULT 'transfer',
    direction        TEXT NOT NULL CHECK (direction IN ('incoming', 'outgoing')),
    amount           TEXT NOT NULL,
    token_symbol     TEXT NOT NULL,
    raw_payload      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_events_wallet_id ON wallet_events(wallet_id);
CREATE INDEX IF NOT EXISTS idx_wallet_events_tx_hash ON wallet_events(tx_hash);
CREATE INDEX IF NOT EXISTS idx_wallet_events_block_number ON wallet_events(block_number);
