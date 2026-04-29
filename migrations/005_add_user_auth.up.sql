-- Migration 005: Add authentication columns to users + refresh_tokens table.
--
-- Module 4 — Aula 1: JWT auth and gRPC parity.
--
-- This migration is intentionally split between users (a single new column)
-- and a brand-new refresh_tokens table. The schema reflects two different
-- token models the service uses:
--
--   * access tokens  — JWT, stateless, short-lived (~15 min). NOT stored.
--   * refresh tokens — opaque random value, hashed at rest, long-lived.
--                      Stored here so they can be rotated and revoked.

-- -----------------------------------------------------------------------------
-- users.password_hash
-- -----------------------------------------------------------------------------
--
-- Argon2id hash in PHC string format, e.g.:
--   $argon2id$v=19$m=65536,t=3,p=2$<salt-b64>$<hash-b64>
--
-- The default is the empty string so the migration is safe to apply on a
-- populated table from earlier modules. Users seeded by 002_seed_data start
-- with empty hashes and cannot log in until a hash is set (the testenv seeder
-- patches them with a known dev hash for integration tests).
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT NOT NULL DEFAULT '';

-- Case-insensitive unique constraint on email.
--
-- Why lower(email) and not just UNIQUE on email? Because users will type
-- "Vitalik@example.com" and "vitalik@example.com" — and we want them to
-- collide. A functional index on lower(email) gives us that without
-- mutating user input.
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_uq ON users (lower(email));

-- -----------------------------------------------------------------------------
-- refresh_tokens
-- -----------------------------------------------------------------------------
--
-- Each row represents an issued refresh token. The token itself is opaque
-- random bytes (32B) shown to the client once at issuance; we only store its
-- SHA-256 hash. SHA-256 (not argon2) is correct here because the input has
-- ~256 bits of entropy — adversarial pre-image attacks are infeasible without
-- a key-stretching cost.
--
-- Rotation chain:
--   replaced_by points at the token that superseded this one. If a refresh
--   request presents a token whose row already has replaced_by IS NOT NULL,
--   we treat it as compromise: the entire chain (walked via replaced_by) is
--   revoked and the user must log in again.
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL,                       -- SHA-256(token), hex
    issued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,                         -- NULL while active
    replaced_by TEXT REFERENCES refresh_tokens(id),  -- rotation lineage
    user_agent  TEXT,
    ip          INET
);

-- A refresh token is looked up by its hash on every /auth/refresh call,
-- so an index on token_hash is critical. UNIQUE because two distinct
-- tokens hashing to the same value would represent a SHA-256 collision.
CREATE UNIQUE INDEX IF NOT EXISTS refresh_tokens_token_hash_uq ON refresh_tokens (token_hash);

-- /auth/logout-all and admin-driven revocation walk a user's tokens.
CREATE INDEX IF NOT EXISTS refresh_tokens_user_id_idx ON refresh_tokens (user_id);
