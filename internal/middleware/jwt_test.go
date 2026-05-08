package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
)

var jwtTestKey = []byte("0123456789abcdef0123456789abcdef")

// downstream is a minimal handler that captures the user id off context
// so the test can assert the middleware really did stash claims.
func downstream(captured *string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid, ok := auth.UserIDFromContext(r.Context())
		if !ok {
			http.Error(w, "no user", http.StatusInternalServerError)
			return
		}
		*captured = uid
		w.WriteHeader(http.StatusOK)
	})
}

func TestJWT_PassesValidTokenAndStashesClaims(t *testing.T) {
	issuer := auth.NewTokenIssuer(jwtTestKey, time.Minute)
	verifier := auth.NewTokenVerifier(jwtTestKey)

	token, _, err := issuer.Issue("u-42")
	require.NoError(t, err)

	var got string
	mw := middleware.JWT(verifier)(downstream(&got))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()

	mw.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "u-42", got, "downstream handler must see claims")
}

func TestJWT_RejectsMissingHeader(t *testing.T) {
	verifier := auth.NewTokenVerifier(jwtTestKey)
	mw := middleware.JWT(verifier)(downstream(new(string)))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	rr := httptest.NewRecorder()
	mw.ServeHTTP(rr, req)

	require.Equal(t, http.StatusUnauthorized, rr.Code)
	require.Contains(t, rr.Body.String(), `"unauthenticated"`)
	require.NotEmpty(t, rr.Header().Get("WWW-Authenticate"))
}

func TestJWT_RejectsMalformedHeader(t *testing.T) {
	verifier := auth.NewTokenVerifier(jwtTestKey)
	mw := middleware.JWT(verifier)(downstream(new(string)))

	cases := []string{
		"basic abc",                  // wrong scheme
		"Bearer ",                    // empty token
		"BearerSomeToken",            // no space separator
		"bearer",                     // scheme alone
	}
	for _, h := range cases {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", h)
		rr := httptest.NewRecorder()
		mw.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnauthorized, rr.Code, "header %q should be rejected", h)
	}
}

func TestJWT_RejectsExpiredToken(t *testing.T) {
	issuer := auth.NewTokenIssuer(jwtTestKey, time.Millisecond)
	verifier := auth.NewTokenVerifier(jwtTestKey)

	token, _, err := issuer.Issue("u-1")
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)

	mw := middleware.JWT(verifier)(downstream(new(string)))
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	mw.ServeHTTP(rr, req)

	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestJWT_AcceptsCaseInsensitiveScheme(t *testing.T) {
	issuer := auth.NewTokenIssuer(jwtTestKey, time.Minute)
	verifier := auth.NewTokenVerifier(jwtTestKey)
	token, _, err := issuer.Issue("u-1")
	require.NoError(t, err)

	var got string
	mw := middleware.JWT(verifier)(downstream(&got))
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "BEARER "+token)
	rr := httptest.NewRecorder()
	mw.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "u-1", got)
}

func TestJWT_PanicsOnNilVerifier(t *testing.T) {
	require.Panics(t, func() {
		middleware.JWT(nil)
	})
}
