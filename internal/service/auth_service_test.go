package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/mocks"
)

// =============================================================================
// fixtures
// =============================================================================

var authSigningKey = []byte("0123456789abcdef0123456789abcdef")

// fastHasher uses minimum argon2id parameters so the test suite stays
// quick. Production code uses NewArgon2idHasher with OWASP defaults.
func fastHasher(t *testing.T) *auth.Argon2idHasher {
	t.Helper()
	return auth.NewArgon2idHasherWithParams(8, 1, 1)
}

// authHarness wires an AuthService with mocked repositories and an
// injected clock so tests can drive time forward deterministically.
type authHarness struct {
	svc      *service.AuthService
	users    *mocks.UserRepository
	refresh  *mocks.RefreshTokenRepository
	hasher   *auth.Argon2idHasher
	now      time.Time
	idCount  int
}

func newAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	h := &authHarness{
		users:   &mocks.UserRepository{},
		refresh: &mocks.RefreshTokenRepository{},
		hasher:  fastHasher(t),
		now:     time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC),
	}

	issuer := auth.NewTokenIssuer(authSigningKey, 5*time.Minute)
	h.svc = service.NewAuthService(h.users, h.refresh, h.hasher, issuer, 24*time.Hour)

	// Replace the unexported clock and id generator. We can't reach those
	// from outside the package, so the package exposes them via the test
	// hooks below instead.
	service.SetClock(h.svc, func() time.Time { return h.now })
	service.SetIDGen(h.svc, func() string {
		h.idCount++
		return fmt.Sprintf("id-%d", h.idCount)
	})
	return h
}

// =============================================================================
// Register
// =============================================================================

func TestAuthService_Register_Success(t *testing.T) {
	h := newAuthHarness(t)

	h.users.On("Create", mock.Anything, mock.MatchedBy(func(u *domain.User) bool {
		return u.Email == "alice@example.com" && u.Name == "Alice"
	}), mock.MatchedBy(func(hash string) bool {
		// hash must be a PHC string and verify against the password
		ok, err := h.hasher.Verify(hash, "correct-horse")
		return err == nil && ok
	})).Return(nil).Once()

	user, err := h.svc.Register(context.Background(), service.RegisterInput{
		Name:     "Alice",
		Email:    "alice@example.com",
		Password: "correct-horse",
	})
	require.NoError(t, err)
	require.NotEmpty(t, user.ID)
	require.Equal(t, "Alice", user.Name)
	require.Equal(t, "alice@example.com", user.Email)
	h.users.AssertExpectations(t)
}

func TestAuthService_Register_RejectsShortPassword(t *testing.T) {
	h := newAuthHarness(t)

	_, err := h.svc.Register(context.Background(), service.RegisterInput{
		Name:     "Alice",
		Email:    "a@b.com",
		Password: "short",
	})
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeValidation, appErr.Code)
	h.users.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything)
}

func TestAuthService_Register_RejectsEmptyEmail(t *testing.T) {
	h := newAuthHarness(t)

	_, err := h.svc.Register(context.Background(), service.RegisterInput{
		Name: "Alice", Email: "  ", Password: "long-enough",
	})
	require.Error(t, err)
}

func TestAuthService_Register_RejectsMalformedEmail(t *testing.T) {
	h := newAuthHarness(t)

	_, err := h.svc.Register(context.Background(), service.RegisterInput{
		Name: "Alice", Email: "not-an-email", Password: "long-enough",
	})
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeValidation, appErr.Code)
}

func TestAuthService_Register_ConflictOnDuplicateEmail(t *testing.T) {
	h := newAuthHarness(t)

	h.users.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(domain.ErrUserAlreadyExists).Once()

	_, err := h.svc.Register(context.Background(), service.RegisterInput{
		Name: "Alice", Email: "alice@example.com", Password: "long-enough",
	})
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeConflict, appErr.Code)
}

// =============================================================================
// Login
// =============================================================================

func TestAuthService_Login_Success(t *testing.T) {
	h := newAuthHarness(t)

	password := "correct-horse"
	pwHash, err := h.hasher.Hash(password)
	require.NoError(t, err)

	h.users.On("FindByEmail", mock.Anything, "alice@example.com").
		Return(&domain.User{ID: "u-1", Email: "alice@example.com", PasswordHash: pwHash}, nil).Once()

	h.refresh.On("Insert", mock.Anything, mock.MatchedBy(func(rt *domain.RefreshToken) bool {
		return rt.UserID == "u-1" && rt.TokenHash != "" && rt.RevokedAt == nil
	})).Return(nil).Once()

	tokens, err := h.svc.Login(context.Background(), service.LoginInput{
		Email:     "alice@example.com",
		Password:  password,
		UserAgent: "test-agent",
		IP:        "127.0.0.1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, tokens.AccessToken)
	require.NotEmpty(t, tokens.RefreshToken)
	require.True(t, tokens.AccessTokenExpiresAt.After(h.now))
	require.True(t, tokens.RefreshTokenExpiresAt.After(h.now))
	h.users.AssertExpectations(t)
	h.refresh.AssertExpectations(t)
}

func TestAuthService_Login_WrongPassword(t *testing.T) {
	h := newAuthHarness(t)

	pwHash, err := h.hasher.Hash("real-password")
	require.NoError(t, err)
	h.users.On("FindByEmail", mock.Anything, "alice@example.com").
		Return(&domain.User{ID: "u-1", PasswordHash: pwHash}, nil).Once()

	_, err = h.svc.Login(context.Background(), service.LoginInput{
		Email: "alice@example.com", Password: "wrong-password",
	})
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	h.refresh.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
}

func TestAuthService_Login_UnknownEmail(t *testing.T) {
	h := newAuthHarness(t)

	h.users.On("FindByEmail", mock.Anything, "ghost@example.com").
		Return(nil, domain.ErrUserNotFound).Once()

	_, err := h.svc.Login(context.Background(), service.LoginInput{
		Email: "ghost@example.com", Password: "any-password",
	})
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	// Importantly, the error message must NOT mention "no such user" so
	// the response is indistinguishable from a wrong-password error.
	require.Equal(t, "authentication required", appErr.Message)
}

func TestAuthService_Login_EmptyInputsReturnUnauthenticated(t *testing.T) {
	h := newAuthHarness(t)

	cases := []service.LoginInput{
		{Email: "", Password: "any"},
		{Email: "a@b.com", Password: ""},
		{Email: "  ", Password: "any"},
	}
	for _, in := range cases {
		_, err := h.svc.Login(context.Background(), in)
		require.Error(t, err)
		var appErr *domain.AppError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	}
	h.users.AssertNotCalled(t, "FindByEmail", mock.Anything, mock.Anything)
}

// =============================================================================
// Refresh — happy path and replay detection
// =============================================================================

func TestAuthService_Refresh_RotatesAndIssuesNewPair(t *testing.T) {
	h := newAuthHarness(t)

	// Set up a valid refresh-token row in the mock store.
	original := "raw-refresh-token-xyz"
	row := &domain.RefreshToken{
		ID:        "rt-old",
		UserID:    "u-1",
		TokenHash: sha256Hex(original),
		IssuedAt:  h.now.Add(-time.Hour),
		ExpiresAt: h.now.Add(24 * time.Hour),
	}
	h.refresh.On("FindByHash", mock.Anything, sha256Hex(original)).Return(row, nil).Once()
	h.refresh.On("Rotate", mock.Anything, "rt-old", mock.MatchedBy(func(rt *domain.RefreshToken) bool {
		return rt.UserID == "u-1" && rt.TokenHash != row.TokenHash
	}), h.now).Return(nil).Once()

	tokens, err := h.svc.Refresh(context.Background(), original, "ua", "1.2.3.4")
	require.NoError(t, err)
	require.NotEqual(t, original, tokens.RefreshToken, "rotated token must differ")
	require.NotEmpty(t, tokens.AccessToken)
	h.refresh.AssertExpectations(t)
	h.refresh.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
}

func TestAuthService_Refresh_DetectsReplayAndRevokesFamily(t *testing.T) {
	h := newAuthHarness(t)

	replayed := "old-rotated-token"
	revokedTime := h.now.Add(-time.Minute)
	row := &domain.RefreshToken{
		ID:        "rt-old",
		UserID:    "u-1",
		TokenHash: sha256Hex(replayed),
		IssuedAt:  h.now.Add(-time.Hour),
		ExpiresAt: h.now.Add(24 * time.Hour),
		RevokedAt: &revokedTime, // already revoked → replay
	}
	h.refresh.On("FindByHash", mock.Anything, sha256Hex(replayed)).Return(row, nil).Once()
	h.refresh.On("RevokeFamily", mock.Anything, "rt-old", h.now).Return(nil).Once()

	_, err := h.svc.Refresh(context.Background(), replayed, "", "")
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	h.refresh.AssertExpectations(t)
	h.refresh.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
	h.refresh.AssertNotCalled(t, "Rotate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestAuthService_Refresh_RaceLossTreatedAsReplay covers the
// concurrent-refresh path: two callers both pass the service-level
// freshness check, but the repository's transactional Rotate detects
// the race at lock time and returns ErrRefreshTokenRevoked. Same
// service response as the explicit replay path: revoke the family,
// return Unauthenticated.
func TestAuthService_Refresh_RaceLossTreatedAsReplay(t *testing.T) {
	h := newAuthHarness(t)

	original := "raw-refresh-token-race"
	row := &domain.RefreshToken{
		ID:        "rt-old",
		UserID:    "u-1",
		TokenHash: sha256Hex(original),
		IssuedAt:  h.now.Add(-time.Hour),
		ExpiresAt: h.now.Add(24 * time.Hour),
	}
	h.refresh.On("FindByHash", mock.Anything, sha256Hex(original)).Return(row, nil).Once()
	h.refresh.On("Rotate", mock.Anything, "rt-old", mock.AnythingOfType("*domain.RefreshToken"), h.now).
		Return(domain.ErrRefreshTokenRevoked).Once()
	h.refresh.On("RevokeFamily", mock.Anything, "rt-old", h.now).Return(nil).Once()

	_, err := h.svc.Refresh(context.Background(), original, "ua", "1.2.3.4")
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	h.refresh.AssertExpectations(t)
}

func TestAuthService_Refresh_RejectsExpiredToken(t *testing.T) {
	h := newAuthHarness(t)

	expired := "expired-token"
	row := &domain.RefreshToken{
		ID:        "rt-old",
		UserID:    "u-1",
		TokenHash: sha256Hex(expired),
		IssuedAt:  h.now.Add(-48 * time.Hour),
		ExpiresAt: h.now.Add(-time.Hour),
	}
	h.refresh.On("FindByHash", mock.Anything, sha256Hex(expired)).Return(row, nil).Once()

	_, err := h.svc.Refresh(context.Background(), expired, "", "")
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
	h.refresh.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
}

func TestAuthService_Refresh_UnknownTokenIsUnauthenticated(t *testing.T) {
	h := newAuthHarness(t)

	h.refresh.On("FindByHash", mock.Anything, mock.Anything).
		Return(nil, domain.ErrRefreshTokenNotFound).Once()

	_, err := h.svc.Refresh(context.Background(), "anything", "", "")
	require.Error(t, err)
	var appErr *domain.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, domain.CodeUnauthenticated, appErr.Code)
}

func TestAuthService_Refresh_EmptyTokenIsUnauthenticated(t *testing.T) {
	h := newAuthHarness(t)
	_, err := h.svc.Refresh(context.Background(), "", "", "")
	require.Error(t, err)
	h.refresh.AssertNotCalled(t, "FindByHash", mock.Anything, mock.Anything)
}

// =============================================================================
// Logout
// =============================================================================

func TestAuthService_Logout_RevokesPresentedTokenOnly(t *testing.T) {
	h := newAuthHarness(t)

	tok := "logout-target"
	row := &domain.RefreshToken{
		ID: "rt-1", UserID: "u-1", TokenHash: sha256Hex(tok),
		IssuedAt: h.now, ExpiresAt: h.now.Add(time.Hour),
	}
	h.refresh.On("FindByHash", mock.Anything, sha256Hex(tok)).Return(row, nil).Once()
	h.refresh.On("Revoke", mock.Anything, "rt-1", h.now).Return(nil).Once()

	require.NoError(t, h.svc.Logout(context.Background(), tok))
	h.refresh.AssertExpectations(t)
	h.refresh.AssertNotCalled(t, "RevokeFamily", mock.Anything, mock.Anything, mock.Anything)
}

func TestAuthService_Logout_UnknownTokenIsNoOp(t *testing.T) {
	h := newAuthHarness(t)

	h.refresh.On("FindByHash", mock.Anything, mock.Anything).
		Return(nil, domain.ErrRefreshTokenNotFound).Once()

	require.NoError(t, h.svc.Logout(context.Background(), "ghost"))
	h.refresh.AssertNotCalled(t, "Revoke", mock.Anything, mock.Anything, mock.Anything)
}

func TestAuthService_Logout_EmptyTokenIsNoOp(t *testing.T) {
	h := newAuthHarness(t)
	require.NoError(t, h.svc.Logout(context.Background(), ""))
	h.refresh.AssertNotCalled(t, "FindByHash", mock.Anything, mock.Anything)
}

// =============================================================================
// Constructor invariants
// =============================================================================

func TestNewAuthService_PanicsOnNilDeps(t *testing.T) {
	hasher := auth.NewArgon2idHasher()
	issuer := auth.NewTokenIssuer(authSigningKey, time.Minute)

	require.Panics(t, func() {
		service.NewAuthService(nil, &mocks.RefreshTokenRepository{}, hasher, issuer, time.Hour)
	})
	require.Panics(t, func() {
		service.NewAuthService(&mocks.UserRepository{}, nil, hasher, issuer, time.Hour)
	})
	require.Panics(t, func() {
		service.NewAuthService(&mocks.UserRepository{}, &mocks.RefreshTokenRepository{}, nil, issuer, time.Hour)
	})
	require.Panics(t, func() {
		service.NewAuthService(&mocks.UserRepository{}, &mocks.RefreshTokenRepository{}, hasher, nil, time.Hour)
	})
	require.Panics(t, func() {
		service.NewAuthService(&mocks.UserRepository{}, &mocks.RefreshTokenRepository{}, hasher, issuer, 0)
	})
}

// =============================================================================
// helpers
// =============================================================================

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
