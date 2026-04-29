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

// MarkRotated marks oldID revoked and links replaced_by → newID, atomically.
//
// "Atomically" matters because the rotation MUST be all-or-nothing: if we
// mark old revoked but never insert new, the user gets logged out; if we
// insert new but never mark old revoked, both tokens are valid and a
// stolen old token would not trigger replay detection. The caller is
// expected to have already inserted newID, but the UPDATE itself is the
// linearisation point.
func (r *RefreshTokenRepository) MarkRotated(ctx context.Context, oldID, newID string, revokedAt time.Time) error {
	const op = "RefreshTokenRepository.MarkRotated"

	query := `UPDATE refresh_tokens
	          SET revoked_at = $1, replaced_by = $2
	          WHERE id = $3`

	res, err := r.db.ExecContext(ctx, query, revokedAt, newID, oldID)
	if err != nil {
		return fmt.Errorf("%s: update failed: %w", op, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: rows affected: %w", op, err)
	}
	if rows == 0 {
		return fmt.Errorf("%s: %w", op, domain.ErrRefreshTokenNotFound)
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
