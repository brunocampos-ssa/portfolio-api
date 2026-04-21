-- Migration 004: Create wallet snapshot tables for the tax report simulation.
-- Captures point-in-time balances and USD values for all tracked ETH wallets.

CREATE TABLE IF NOT EXISTS wallet_snapshots (
    id             TEXT PRIMARY KEY,
    reference_time TIMESTAMPTZ NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_snapshot_items (
    id               TEXT PRIMARY KEY,
    snapshot_id      TEXT NOT NULL REFERENCES wallet_snapshots(id) ON DELETE CASCADE,
    wallet_id        TEXT NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    asset_symbol     TEXT NOT NULL,
    asset_type       TEXT NOT NULL CHECK (asset_type IN ('native', 'erc20')),
    contract_address TEXT NOT NULL DEFAULT '',
    amount           DOUBLE PRECISION NOT NULL DEFAULT 0,
    usd_price        DOUBLE PRECISION NOT NULL DEFAULT 0,
    usd_value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_items_snapshot_id ON wallet_snapshot_items(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_items_wallet_id ON wallet_snapshot_items(wallet_id);
