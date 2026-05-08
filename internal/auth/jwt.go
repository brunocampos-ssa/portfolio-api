package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// =============================================================================
// JWT access tokens — HS256, stateless
// =============================================================================
//
// Access tokens authorise API calls. They are stateless: the server only
// needs the signing key to verify them. That has two consequences students
// should internalise:
//
//   1. Anyone holding the signing key can mint tokens. Treat it like a
//      database password — secrets manager only, never in source.
//   2. Revoking a leaked access token is hard. The mitigation is short
//      lifetimes (~15 min) plus refresh tokens (which ARE server-side state
//      and CAN be revoked instantly — see refresh_token_repository).
//
// We use HS256 (HMAC-SHA-256). For a single service this is the simpler
// choice. RS256 (asymmetric) makes sense once a separate auth service mints
// tokens for many resource servers — that's a different lesson.

const (
	// Issuer is the value placed in the JWT iss claim and validated on verify.
	// Hard-coded because it is a deployment-wide identifier, not user input.
	Issuer = "portfolio-api"

	// DefaultAccessTokenTTL — short by design. Refresh tokens cover the gap.
	DefaultAccessTokenTTL = 15 * time.Minute
)

// Claims is the project's view of what an access token carries.
//
// We deliberately wrap jwt.RegisteredClaims rather than use it directly so
// the rest of the codebase depends on OUR shape, not the upstream library's.
// If we ever need to attach custom claims (roles, tenant, ...), they go here
// and downstream code changes nothing.
type Claims struct {
	// Subject is the user ID. It is the source of truth for "who is this
	// request" — handlers compare it against the path id when self-access
	// is required.
	Subject string

	// IssuedAt and ExpiresAt come straight from the JWT iat/exp claims.
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// =============================================================================
// Issuer
// =============================================================================

// TokenIssuer mints access tokens.
type TokenIssuer struct {
	signingKey []byte
	ttl        time.Duration
	now        func() time.Time // injectable for tests
}

// NewTokenIssuer constructs a TokenIssuer.
//
// signingKey must be at least 32 bytes — HS256 with shorter keys is weaker
// than the algorithm advertises and the constructor refuses to issue from
// such configurations. Configure via env var (JWT_SIGNING_KEY) and never
// commit a real key.
func NewTokenIssuer(signingKey []byte, ttl time.Duration) *TokenIssuer {
	if len(signingKey) < 32 {
		panic("auth.NewTokenIssuer: signingKey must be at least 32 bytes")
	}
	if ttl <= 0 {
		panic("auth.NewTokenIssuer: ttl must be positive")
	}
	return &TokenIssuer{
		signingKey: signingKey,
		ttl:        ttl,
		now:        time.Now,
	}
}

// Issue returns a signed access token whose subject is the given user ID.
//
// The returned ExpiresAt is exposed so callers (e.g., the login handler)
// can include it in the response body — clients use it to schedule a
// refresh slightly before expiry rather than reacting to a 401.
func (i *TokenIssuer) Issue(userID string) (token string, expiresAt time.Time, err error) {
	if userID == "" {
		return "", time.Time{}, errors.New("auth: userID must not be empty")
	}

	now := i.now()
	expiresAt = now.Add(i.ttl)

	claims := jwt.RegisteredClaims{
		Subject:   userID,
		Issuer:    Issuer,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(expiresAt),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signed, err := t.SignedString(i.signingKey)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, expiresAt, nil
}

// =============================================================================
// Verifier
// =============================================================================

// ErrInvalidToken is returned for any verification failure. We deliberately
// collapse "expired", "bad signature", "wrong issuer", etc. into one error
// so the HTTP/gRPC layer cannot accidentally leak which check failed —
// that distinction would help an attacker probe for valid tokens.
var ErrInvalidToken = errors.New("invalid or expired token")

// TokenVerifier validates access tokens and extracts claims.
type TokenVerifier struct {
	signingKey []byte
}

// NewTokenVerifier constructs a verifier with the given signing key.
func NewTokenVerifier(signingKey []byte) *TokenVerifier {
	if len(signingKey) < 32 {
		panic("auth.NewTokenVerifier: signingKey must be at least 32 bytes")
	}
	return &TokenVerifier{signingKey: signingKey}
}

// Verify parses and validates the token, returning the project's Claims
// shape on success.
//
// All failures collapse to ErrInvalidToken. The wrapped error can be
// inspected internally (logs) but must NOT be exposed to clients.
func (v *TokenVerifier) Verify(tokenString string) (*Claims, error) {
	if tokenString == "" {
		return nil, ErrInvalidToken
	}

	parsed, err := jwt.ParseWithClaims(
		tokenString,
		&jwt.RegisteredClaims{},
		func(t *jwt.Token) (any, error) {
			// Pin the algorithm. Without this, an attacker could send a
			// token signed with "none" or with our public key as if it
			// were HMAC and pass verification — the classic JWT footgun.
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return v.signingKey, nil
		},
		jwt.WithIssuer(Issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !parsed.Valid {
		return nil, ErrInvalidToken
	}

	rc, ok := parsed.Claims.(*jwt.RegisteredClaims)
	if !ok || rc.Subject == "" || rc.ExpiresAt == nil || rc.IssuedAt == nil {
		return nil, ErrInvalidToken
	}

	return &Claims{
		Subject:   rc.Subject,
		IssuedAt:  rc.IssuedAt.Time,
		ExpiresAt: rc.ExpiresAt.Time,
	}, nil
}

// =============================================================================
// Context propagation
// =============================================================================
//
// The middleware/interceptor stash claims into request context after a
// successful verify. Handlers and services pull the current user out via
// UserIDFromContext / ClaimsFromContext.

type claimsKey struct{}

// WithClaims returns a copy of ctx carrying the given claims.
func WithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// ClaimsFromContext returns the claims stored on ctx, if any.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*Claims)
	return c, ok
}

// UserIDFromContext is a convenience for the common case where callers
// only want the authenticated user ID.
func UserIDFromContext(ctx context.Context) (string, bool) {
	c, ok := ClaimsFromContext(ctx)
	if !ok {
		return "", false
	}
	return c.Subject, true
}
