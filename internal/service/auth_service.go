package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// AuthService — registration, login, token rotation, logout
// =============================================================================
//
// AuthService is the application's policy layer for authentication. The
// transports (REST handlers and gRPC interceptor) call into it; the
// crypto-aware bits (argon2id, JWT, refresh-token storage) are isolated
// in dedicated packages so this file stays focused on the workflow.
//
// Two-token model recap:
//
//   * Access token  — JWT, stateless, ~15 min, sent on every request.
//   * Refresh token — opaque random 32 bytes, stored as SHA-256 hash,
//                     ~30 days, exchanged for a fresh access token.
//
// Refresh tokens are rotated on every use: each /auth/refresh issues a
// brand-new refresh token and revokes the presented one. Replay of a
// revoked token is treated as compromise and revokes the whole chain.

const (
	// DefaultRefreshTokenTTL — long enough that "stay signed in" feels
	// natural, short enough that a stolen token has finite life even if
	// the user never logs out.
	DefaultRefreshTokenTTL = 30 * 24 * time.Hour

	// MinPasswordLen is checked at registration only. Stronger passwords
	// stay accepted; we just refuse the obviously-weak end of the range.
	MinPasswordLen = 8

	refreshTokenBytes = 32
)

// Tokens is the pair returned by Login and Refresh.
//
// Both tokens are returned exactly once; the client is responsible for
// storing them. Clients should treat AccessTokenExpiresAt as a hint —
// schedule a refresh slightly before it, rather than reacting to a 401.
type Tokens struct {
	AccessToken           string
	AccessTokenExpiresAt  time.Time
	RefreshToken          string
	RefreshTokenExpiresAt time.Time
}

// RegisterInput is the data needed to create a new account.
type RegisterInput struct {
	ID       string // assigned by caller; allows tests to use stable IDs
	Name     string
	Email    string
	Password string
}

// LoginInput is the credentials + request metadata needed to log in.
//
// UserAgent and IP are recorded with the issued refresh token so the
// rotation audit trail is meaningful. They are optional — empty strings
// translate to NULL in the database.
type LoginInput struct {
	Email     string
	Password  string
	UserAgent string
	IP        string
}

// AuthService orchestrates registration, login, refresh, and logout.
type AuthService struct {
	users      contracts.UserRepository
	refresh    contracts.RefreshTokenRepository
	hasher     *auth.Argon2idHasher
	issuer     *auth.TokenIssuer
	refreshTTL time.Duration

	// Injected for tests so timestamps and IDs are deterministic. Real
	// callers leave them at their zero value and the service substitutes
	// time.Now and a random-id generator at construction time.
	now   func() time.Time
	idGen func() string
}

// NewAuthService wires the auth service. All dependencies are mandatory.
func NewAuthService(
	users contracts.UserRepository,
	refresh contracts.RefreshTokenRepository,
	hasher *auth.Argon2idHasher,
	issuer *auth.TokenIssuer,
	refreshTTL time.Duration,
) *AuthService {
	if users == nil {
		panic("service.NewAuthService: users must not be nil")
	}
	if refresh == nil {
		panic("service.NewAuthService: refresh must not be nil")
	}
	if hasher == nil {
		panic("service.NewAuthService: hasher must not be nil")
	}
	if issuer == nil {
		panic("service.NewAuthService: issuer must not be nil")
	}
	if refreshTTL <= 0 {
		panic("service.NewAuthService: refreshTTL must be positive")
	}
	return &AuthService{
		users:      users,
		refresh:    refresh,
		hasher:     hasher,
		issuer:     issuer,
		refreshTTL: refreshTTL,
		now:        time.Now,
		idGen:      defaultIDGen,
	}
}

// =============================================================================
// Register
// =============================================================================

// Register creates a new user account.
//
// It does NOT auto-login. OWASP recommends a separate authentication step
// after registration so the same code path is exercised on every login —
// which means flaws in Login surface immediately, rather than silently
// during a "sign up and continue" flow that bypasses parts of the check.
func (s *AuthService) Register(ctx context.Context, in RegisterInput) (*domain.User, error) {
	const op = "AuthService.Register"

	in.Name = strings.TrimSpace(in.Name)
	in.Email = strings.TrimSpace(in.Email)

	if in.ID == "" {
		in.ID = "u_" + s.idGen()
	}
	if in.Name == "" {
		return nil, domain.NewValidationError(op, "name is required")
	}
	if in.Email == "" {
		return nil, domain.NewValidationError(op, "email is required")
	}
	if !looksLikeEmail(in.Email) {
		return nil, domain.NewValidationError(op, "email is not a valid address")
	}
	if len(in.Password) < MinPasswordLen {
		return nil, domain.NewValidationError(op, fmt.Sprintf("password must be at least %d characters", MinPasswordLen))
	}

	hash, err := s.hasher.Hash(in.Password)
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	user := &domain.User{
		ID:    in.ID,
		Name:  in.Name,
		Email: in.Email,
	}

	if err := s.users.Create(ctx, user, hash); err != nil {
		if errors.Is(err, domain.ErrUserAlreadyExists) {
			return nil, domain.NewConflictError(op, "email already registered", err)
		}
		return nil, domain.NewInternalError(op, err)
	}

	return user, nil
}

// =============================================================================
// Login
// =============================================================================

// Login validates credentials and issues a token pair.
//
// Constant-timing on credential check: when the email does not exist we
// still run the argon2id verify against a known dummy hash so the timing
// of "no such email" matches "wrong password". This denies an attacker
// the ability to enumerate accounts by observing response latency.
//
// Both failure modes collapse into ErrInvalidCredentials. The caller maps
// it to 401 / Unauthenticated without knowing which check tripped.
func (s *AuthService) Login(ctx context.Context, in LoginInput) (*Tokens, error) {
	const op = "AuthService.Login"

	in.Email = strings.TrimSpace(in.Email)
	if in.Email == "" || in.Password == "" {
		// Still run the dummy verify to keep timing constant.
		_, _ = s.hasher.Verify(dummyArgon2idHash, "any")
		return nil, domain.NewUnauthenticatedError(op, domain.ErrInvalidCredentials)
	}

	user, err := s.users.FindByEmail(ctx, in.Email)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			_, _ = s.hasher.Verify(dummyArgon2idHash, in.Password)
			return nil, domain.NewUnauthenticatedError(op, domain.ErrInvalidCredentials)
		}
		return nil, domain.NewInternalError(op, err)
	}

	ok, err := s.hasher.Verify(user.PasswordHash, in.Password)
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}
	if !ok {
		return nil, domain.NewUnauthenticatedError(op, domain.ErrInvalidCredentials)
	}

	return s.issueTokens(ctx, user.ID, in.UserAgent, in.IP, "")
}

// =============================================================================
// Refresh — with rotation and replay detection
// =============================================================================

// Refresh exchanges a valid refresh token for a fresh access+refresh pair.
//
// Replay detection is the meaty teaching moment in Module 4. The contract:
//
//   1. Look up the row by SHA-256 hash of the presented token.
//   2. If the row's revoked_at is already set, the same token was used
//      before. That means either:
//        a) The legitimate client is replaying its own old token (bug or
//           clock skew) — but the new one was already issued and is what
//           the client should be using.
//        b) An attacker stole an old token after rotation.
//      We cannot tell (a) from (b), so we treat both as compromise and
//      revoke the entire chain. The legitimate user has to log in again.
//   3. If the row is expired, treat as invalid.
//   4. Otherwise: mint a new access+refresh pair, mark the old token
//      revoked + replaced_by the new id.
//
// All failure paths surface as ErrInvalidCredentials so the response is
// indistinguishable.
func (s *AuthService) Refresh(ctx context.Context, refreshToken, userAgent, ip string) (*Tokens, error) {
	const op = "AuthService.Refresh"

	if refreshToken == "" {
		return nil, domain.NewUnauthenticatedError(op, domain.ErrInvalidCredentials)
	}

	hash := hashRefreshToken(refreshToken)
	now := s.now()

	row, err := s.refresh.FindByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, domain.ErrRefreshTokenNotFound) {
			return nil, domain.NewUnauthenticatedError(op, domain.ErrInvalidCredentials)
		}
		return nil, domain.NewInternalError(op, err)
	}

	if row.RevokedAt != nil {
		// Replay. Burn the chain.
		if err := s.refresh.RevokeFamily(ctx, row.ID, now); err != nil {
			return nil, domain.NewInternalError(op, err)
		}
		return nil, domain.NewUnauthenticatedError(op, domain.ErrRefreshTokenRevoked)
	}

	if !now.Before(row.ExpiresAt) {
		return nil, domain.NewUnauthenticatedError(op, domain.ErrRefreshTokenExpired)
	}

	tokens, err := s.issueTokens(ctx, row.UserID, userAgent, ip, row.ID)
	if errors.Is(err, domain.ErrRefreshTokenRevoked) {
		// Race-lost replay: a concurrent /auth/refresh of the same token
		// committed first. The repository's row lock surfaced this as
		// ErrRefreshTokenRevoked. Same response as the explicit
		// service-level replay path: revoke the family and deny.
		if revokeErr := s.refresh.RevokeFamily(ctx, row.ID, now); revokeErr != nil {
			return nil, domain.NewInternalError(op, revokeErr)
		}
		return nil, domain.NewUnauthenticatedError(op, domain.ErrRefreshTokenRevoked)
	}
	return tokens, err
}

// =============================================================================
// Logout
// =============================================================================

// Logout revokes the presented refresh token.
//
// Idempotent: revoking an unknown or already-revoked token is a no-op
// from the client's perspective. We deliberately do NOT error on
// "token not found" — that would tell an attacker which random strings
// happen to exist as refresh tokens.
//
// Logout revokes ONLY the specific token presented, not the whole chain
// or the user's other sessions. Logging the user out everywhere is a
// separate operation (deferred to Module 4 Aula 2 alongside admin/audit).
func (s *AuthService) Logout(ctx context.Context, refreshToken string) error {
	const op = "AuthService.Logout"

	if refreshToken == "" {
		return nil
	}

	hash := hashRefreshToken(refreshToken)
	row, err := s.refresh.FindByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, domain.ErrRefreshTokenNotFound) {
			return nil
		}
		return domain.NewInternalError(op, err)
	}

	if err := s.refresh.Revoke(ctx, row.ID, s.now()); err != nil {
		return domain.NewInternalError(op, err)
	}
	return nil
}

// =============================================================================
// helpers
// =============================================================================

// issueTokens mints a new access+refresh pair and persists the refresh
// row. When rotateFromID is empty (login), the new row is plainly
// inserted. When rotateFromID is set (refresh), the repository's
// transactional Rotate runs INSERT + UPDATE under a SELECT FOR UPDATE
// lock on the old row, so concurrent rotations of the same token cannot
// race past each other.
//
// On race-loss the repository surfaces domain.ErrRefreshTokenRevoked
// unwrapped so the caller (Refresh) can distinguish "the rotation lost
// the race" from a generic DB failure and respond with the same
// revoke-the-family treatment as the explicit replay path.
func (s *AuthService) issueTokens(ctx context.Context, userID, userAgent, ip, rotateFromID string) (*Tokens, error) {
	const op = "AuthService.issueTokens"

	access, accessExp, err := s.issuer.Issue(userID)
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	rawRefresh, err := generateRefreshToken()
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	now := s.now()
	row := &domain.RefreshToken{
		ID:        "rt_" + s.idGen(),
		UserID:    userID,
		TokenHash: hashRefreshToken(rawRefresh),
		IssuedAt:  now,
		ExpiresAt: now.Add(s.refreshTTL),
	}
	if userAgent != "" {
		ua := userAgent
		row.UserAgent = &ua
	}
	if ip != "" {
		ipv := ip
		row.IP = &ipv
	}

	if rotateFromID == "" {
		if err := s.refresh.Insert(ctx, row); err != nil {
			return nil, domain.NewInternalError(op, err)
		}
	} else {
		if err := s.refresh.Rotate(ctx, rotateFromID, row, now); err != nil {
			// Bubble race-loss / not-found unwrapped so Refresh can react.
			if errors.Is(err, domain.ErrRefreshTokenRevoked) ||
				errors.Is(err, domain.ErrRefreshTokenNotFound) {
				return nil, err
			}
			return nil, domain.NewInternalError(op, err)
		}
	}

	return &Tokens{
		AccessToken:           access,
		AccessTokenExpiresAt:  accessExp,
		RefreshToken:          rawRefresh,
		RefreshTokenExpiresAt: row.ExpiresAt,
	}, nil
}

// generateRefreshToken returns a cryptographically random base64url string
// with no padding. 32 random bytes → ~256 bits of entropy → SHA-256
// collisions are infeasible.
func generateRefreshToken() (string, error) {
	b := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: read refresh entropy: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// hashRefreshToken returns hex(SHA-256(token)). Hex (not base64) so the
// stored value is index-friendly and trivially comparable in psql.
func hashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// looksLikeEmail is a deliberately conservative check — full RFC 5322
// validation is overkill for a teaching codebase and the unique index
// catches anything that slips through.
func looksLikeEmail(s string) bool {
	at := strings.IndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return false
	}
	if strings.IndexByte(s[at+1:], '.') < 0 {
		return false
	}
	return true
}

// dummyArgon2idHash is used by Login to keep verification timing constant
// when the email is unknown. It is a precomputed argon2id digest with the
// project's default parameters (m=65536, t=3, p=2) and a fixed all-zero
// salt — visible in the encoded string. The exact plaintext doesn't
// matter for the constant-timing property; what matters is that the
// digest is real and the verify cost equals a genuine login.
//
// Hard-coded as a string so the constant-timing path has no startup
// dependency on running argon2.
const dummyArgon2idHash = "$argon2id$v=19$m=65536,t=3,p=2$AAAAAAAAAAAAAAAAAAAAAA$5VxJ/0HMftL5LwXbFVE/g/oCemo+OS1JGVJUSCMpb2Y"

// defaultIDGen returns 16 random bytes hex-encoded. Call sites prepend a
// short prefix ("u_" for users, "rt_" for refresh tokens) so IDs are
// self-describing in logs. 16 random bytes is collision-free at any
// realistic scale.
func defaultIDGen() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Should never happen; falling back to a timestamp keeps the
		// service running in the truly-degenerate case.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
