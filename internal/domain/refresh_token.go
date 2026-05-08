package domain

import "time"

// RefreshToken is a server-side record of an issued long-lived token.
//
// The token value itself is shown to the client exactly once, at issuance,
// and is NOT stored anywhere on the server — only its SHA-256 hash. This
// is symmetric to how websites store password hashes and matches OWASP
// guidance for server-side session identifiers.
//
// Rotation chain:
//
//	Each successful /auth/refresh marks the presented token revoked and
//	mints a new one. ReplacedBy on the old row points at the new row's
//	ID. If a refresh request ever presents a token whose row already has
//	ReplacedBy set, the system treats it as a replay and revokes the
//	entire chain (see service.AuthService.Refresh).
type RefreshToken struct {
	ID          string     `db:"id"`
	UserID      string     `db:"user_id"`
	TokenHash   string     `db:"token_hash"`   // SHA-256(token), hex
	IssuedAt    time.Time  `db:"issued_at"`
	ExpiresAt   time.Time  `db:"expires_at"`
	RevokedAt   *time.Time `db:"revoked_at"`   // nil while the token is active
	ReplacedBy  *string    `db:"replaced_by"`  // nil unless rotated
	UserAgent   *string    `db:"user_agent"`
	IP          *string    `db:"ip"`
}

// IsActive reports whether the token can currently be used to mint a new
// access token. A token is active when it is neither revoked nor expired.
func (t *RefreshToken) IsActive(now time.Time) bool {
	if t.RevokedAt != nil {
		return false
	}
	return now.Before(t.ExpiresAt)
}
