package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
)

// Reasonable 32-byte test key — fixed value so tests are deterministic.
var testSigningKey = []byte("0123456789abcdef0123456789abcdef")

func TestTokenIssuer_Issue_RoundTrip(t *testing.T) {
	issuer := auth.NewTokenIssuer(testSigningKey, 5*time.Minute)
	verifier := auth.NewTokenVerifier(testSigningKey)

	token, expiresAt, err := issuer.Issue("u-42")
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.True(t, expiresAt.After(time.Now()), "expiresAt should be in the future")
	require.Len(t, strings.Split(token, "."), 3, "JWT must have header.payload.signature")

	claims, err := verifier.Verify(token)
	require.NoError(t, err)
	require.Equal(t, "u-42", claims.Subject)
	require.WithinDuration(t, expiresAt, claims.ExpiresAt, time.Second)
	require.False(t, claims.IssuedAt.IsZero())
}

func TestTokenIssuer_Issue_RejectsEmptySubject(t *testing.T) {
	issuer := auth.NewTokenIssuer(testSigningKey, time.Minute)
	_, _, err := issuer.Issue("")
	require.Error(t, err)
}

func TestTokenVerifier_Verify_RejectsExpiredToken(t *testing.T) {
	// Forge a token with exp in the past by signing it manually.
	claims := jwt.RegisteredClaims{
		Subject:   "u-1",
		Issuer:    auth.Issuer,
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(testSigningKey)
	require.NoError(t, err)

	verifier := auth.NewTokenVerifier(testSigningKey)
	_, err = verifier.Verify(signed)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestTokenVerifier_Verify_RejectsWrongSigningKey(t *testing.T) {
	issuer := auth.NewTokenIssuer(testSigningKey, time.Minute)
	token, _, err := issuer.Issue("u-1")
	require.NoError(t, err)

	otherKey := []byte("ffffffffffffffffffffffffffffffff")
	verifier := auth.NewTokenVerifier(otherKey)
	_, err = verifier.Verify(token)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestTokenVerifier_Verify_RejectsTamperedPayload(t *testing.T) {
	issuer := auth.NewTokenIssuer(testSigningKey, time.Minute)
	token, _, err := issuer.Issue("u-1")
	require.NoError(t, err)

	// Flip a character in the payload segment to invalidate the signature.
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	if parts[1][0] == 'A' {
		parts[1] = "B" + parts[1][1:]
	} else {
		parts[1] = "A" + parts[1][1:]
	}
	tampered := strings.Join(parts, ".")

	verifier := auth.NewTokenVerifier(testSigningKey)
	_, err = verifier.Verify(tampered)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestTokenVerifier_Verify_RejectsWrongIssuer(t *testing.T) {
	// Manually mint a token with a foreign issuer but our signing key.
	claims := jwt.RegisteredClaims{
		Subject:   "u-1",
		Issuer:    "some-other-service",
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(testSigningKey)
	require.NoError(t, err)

	verifier := auth.NewTokenVerifier(testSigningKey)
	_, err = verifier.Verify(signed)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestTokenVerifier_Verify_RejectsAlgNone(t *testing.T) {
	// Classic JWT footgun: an attacker swaps the alg to "none". Our
	// keyfunc pins HS256, so the parse must reject it.
	claims := jwt.RegisteredClaims{
		Subject:   "u-1",
		Issuer:    auth.Issuer,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}
	t2 := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	signed, err := t2.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	verifier := auth.NewTokenVerifier(testSigningKey)
	_, err = verifier.Verify(signed)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestTokenVerifier_Verify_RejectsEmptyString(t *testing.T) {
	verifier := auth.NewTokenVerifier(testSigningKey)
	_, err := verifier.Verify("")
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestNewTokenIssuer_RejectsShortKey(t *testing.T) {
	require.Panics(t, func() {
		auth.NewTokenIssuer([]byte("too-short"), time.Minute)
	})
}

func TestNewTokenIssuer_RejectsNonPositiveTTL(t *testing.T) {
	require.Panics(t, func() {
		auth.NewTokenIssuer(testSigningKey, 0)
	})
	require.Panics(t, func() {
		auth.NewTokenIssuer(testSigningKey, -time.Second)
	})
}

func TestNewTokenVerifier_RejectsShortKey(t *testing.T) {
	require.Panics(t, func() {
		auth.NewTokenVerifier([]byte("too-short"))
	})
}

func TestContextHelpers_RoundTrip(t *testing.T) {
	c := &auth.Claims{Subject: "u-7", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
	ctx := auth.WithClaims(context.Background(), c)

	got, ok := auth.ClaimsFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, c, got)

	uid, ok := auth.UserIDFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "u-7", uid)
}

func TestContextHelpers_BareContext(t *testing.T) {
	_, ok := auth.ClaimsFromContext(context.Background())
	require.False(t, ok)

	uid, ok := auth.UserIDFromContext(context.Background())
	require.False(t, ok)
	require.Empty(t, uid)
}
