package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// RefreshTokenRepository implements contracts.RefreshTokenRepository.
type RefreshTokenRepository struct {
	db *sql.DB
}

// NewRefreshTokenRepository constructs a postgres-backed refresh-token store.
func NewRefreshTokenRepository(db *sql.DB) *RefreshTokenRepository {
	if db == nil {
		panic("postgres.NewRefreshTokenRepository: db must not be nil")
	}
	return &RefreshTokenRepository{db: db}
}

// Insert persists a freshly issued refresh token.
//
// Conflicts on token_hash would mean two distinct random tokens collided
// under SHA-256 — astronomically unlikely. We surface the conflict as a
// generic insert failure rather than a custom error because no normal
// caller can recover from it.
func (r *RefreshTokenRepository) Insert(ctx context.Context, t *domain.RefreshToken) error {
	const op = "RefreshTokenRepository.Insert"

	query := `INSERT INTO refresh_tokens
	          (id, user_id, token_hash, issued_at, expires_at, revoked_at, replaced_by, user_agent, ip)
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := r.db.ExecContext(ctx, query,
		t.ID, t.UserID, t.TokenHash, t.IssuedAt, t.ExpiresAt,
		t.RevokedAt, t.ReplacedBy, t.UserAgent, t.IP,
	)
	if err != nil {
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}
	return nil
}

// FindByHash returns the refresh-token row whose token_hash matches.
//
// "No rows" → domain.ErrRefreshTokenNotFound, mirroring how UserRepository
// translates sql.ErrNoRows.
func (r *RefreshTokenRepository) FindByHash(ctx context.Context, tokenHash string) (*domain.RefreshToken, error) {
	const op = "RefreshTokenRepository.FindByHash"

	query := `SELECT id, user_id, token_hash, issued_at, expires_at, revoked_at, replaced_by, user_agent, ip
	          FROM refresh_tokens
	          WHERE token_hash = $1`

	t := &domain.RefreshToken{}
	err := r.db.QueryRowContext(ctx, query, tokenHash).Scan(
		&t.ID, &t.UserID, &t.TokenHash, &t.IssuedAt, &t.ExpiresAt,
		&t.RevokedAt, &t.ReplacedBy, &t.UserAgent, &t.IP,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, domain.ErrRefreshTokenNotFound)
		}
		return nil, fmt.Errorf("%s: query failed: %w", op, err)
	}
	return t, nil
}

// Rotate atomically inserts newToken and marks oldID revoked + linked to
// newToken.ID, in a single transaction.
//
// "Atomically" matters for two reasons:
//
//  1. All-or-nothing rotation: if we mark old revoked but never insert new,
//     the user gets logged out; if we insert new but never mark old
//     revoked, both tokens are valid and a stolen old token would not
//     trigger replay detection.
//
//  2. Concurrency: two concurrent refreshes of the same token can both
//     pass the service-level freshness check (row.RevokedAt == nil) and
//     race here. Without a row lock, both would insert their own new
//     token and both would succeed at UPDATE — the loser's new token
//     becomes an orphan: valid, unrevoked, and unreachable from
//     RevokeFamily(oldID) because replaced_by points at the winner.
//
// We solve both by opening a transaction, taking SELECT ... FOR UPDATE on
// the old row, asserting it is still fresh, then INSERT + UPDATE within
// the same tx and committing. A concurrent caller blocks on the row lock
// until we commit, then sees revoked_at != NULL and returns
// ErrRefreshTokenRevoked — which AuthService.Refresh treats as replay.
//
// Returns ErrRefreshTokenNotFound if oldID is not in the table at all
// (caller bug — Refresh has already FindByHash'd) and
// ErrRefreshTokenRevoked if the row is already revoked or already linked
// (lost the rotation race / genuine replay).
func (r *RefreshTokenRepository) Rotate(ctx context.Context, oldID string, newToken *domain.RefreshToken, revokedAt time.Time) error {
	const op = "RefreshTokenRepository.Rotate"

	if newToken == nil {
		return fmt.Errorf("%s: newToken must not be nil", op)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: begin tx: %w", op, err)
	}
	// Rollback is a no-op after a successful Commit; safe in either path.
	defer func() { _ = tx.Rollback() }()

	// Lock the old row so concurrent rotations of the same token serialise.
	var existingRevokedAt *time.Time
	var existingReplacedBy *string
	err = tx.QueryRowContext(ctx,
		`SELECT revoked_at, replaced_by FROM refresh_tokens WHERE id = $1 FOR UPDATE`,
		oldID,
	).Scan(&existingRevokedAt, &existingReplacedBy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s: %w", op, domain.ErrRefreshTokenNotFound)
		}
		return fmt.Errorf("%s: lock old: %w", op, err)
	}
	if existingRevokedAt != nil || existingReplacedBy != nil {
		return fmt.Errorf("%s: %w", op, domain.ErrRefreshTokenRevoked)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO refresh_tokens
		    (id, user_id, token_hash, issued_at, expires_at, revoked_at, replaced_by, user_agent, ip)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		newToken.ID, newToken.UserID, newToken.TokenHash, newToken.IssuedAt, newToken.ExpiresAt,
		newToken.RevokedAt, newToken.ReplacedBy, newToken.UserAgent, newToken.IP,
	); err != nil {
		return fmt.Errorf("%s: insert new: %w", op, err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE refresh_tokens SET revoked_at = $1, replaced_by = $2 WHERE id = $3`,
		revokedAt, newToken.ID, oldID,
	); err != nil {
		return fmt.Errorf("%s: rotate old: %w", op, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: commit: %w", op, err)
	}
	return nil
}

// Revoke marks a single token revoked. Already-revoked tokens are left as-is
// (idempotent) so a logout retry cannot trigger replay detection.
func (r *RefreshTokenRepository) Revoke(ctx context.Context, id string, revokedAt time.Time) error {
	const op = "RefreshTokenRepository.Revoke"

	query := `UPDATE refresh_tokens
	          SET revoked_at = COALESCE(revoked_at, $1)
	          WHERE id = $2`

	if _, err := r.db.ExecContext(ctx, query, revokedAt, id); err != nil {
		return fmt.Errorf("%s: update failed: %w", op, err)
	}
	return nil
}

// RevokeFamily walks the chain reachable from startID — both directions —
// and revokes every token along the way.
//
// Each row has at most one replaced_by, so the lineage is a linked list:
//
//	[older] ← replaced_by ← [middle] ← replaced_by ← [newer]
//
// "forward" walks newer-to-older by following replaced_by pointers from
// startID. "backward" walks older-to-newer by finding rows whose
// replaced_by equals an already-discovered id. Together they cover the
// full chain that contains startID, in a single round trip.
//
// COALESCE preserves the original revoked_at on tokens that were already
// revoked — keeps the timeline accurate when the same chain is hit twice.
func (r *RefreshTokenRepository) RevokeFamily(ctx context.Context, startID string, revokedAt time.Time) error {
	const op = "RefreshTokenRepository.RevokeFamily"

	query := `WITH RECURSIVE
	          forward AS (
	              SELECT id, replaced_by FROM refresh_tokens WHERE id = $1
	            UNION
	              SELECT rt.id, rt.replaced_by
	              FROM refresh_tokens rt
	              JOIN forward f ON rt.id = f.replaced_by
	          ),
	          backward AS (
	              SELECT id FROM refresh_tokens WHERE id = $1
	            UNION
	              SELECT rt.id
	              FROM refresh_tokens rt
	              JOIN backward b ON rt.replaced_by = b.id
	          )
	          UPDATE refresh_tokens
	          SET revoked_at = COALESCE(revoked_at, $2)
	          WHERE id IN (SELECT id FROM forward)
	             OR id IN (SELECT id FROM backward)`

	if _, err := r.db.ExecContext(ctx, query, startID, revokedAt); err != nil {
		return fmt.Errorf("%s: revoke family failed: %w", op, err)
	}
	return nil
}
