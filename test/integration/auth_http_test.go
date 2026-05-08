//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// REST end-to-end auth tests
// =============================================================================
//
// Each test boots its own HTTP server in newAuthStack but shares the
// integration Postgres. Emails are unique per test so collisions on the
// users.email index are impossible.
//
// What gets covered:
//
//   * Happy path: register → login → access protected endpoint → refresh
//     → access again with rotated token → logout
//   * Replay detection: presenting an already-rotated refresh token
//     returns 401 AND revokes the entire chain on the server
//   * Cross-account guard: u1's token cannot read u2's portfolio
//   * Constant-timing on bad credentials: no oracle for unknown email vs
//     wrong password (asserted by status code + message equality)

func TestHTTPAuth_HappyPath(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "happy")

	// --- register
	user := httpRegister(t, stack, email, "Happy User", "correct-horse-battery")

	// --- login
	tokens := httpLogin(t, stack, email, "correct-horse-battery")
	require.NotEmpty(t, tokens["access_token"])
	require.NotEmpty(t, tokens["refresh_token"])

	// --- protected call with access token
	body := httpGetJSON(t, stack, "/users/"+user["id"].(string)+"/portfolio",
		tokens["access_token"].(string), http.StatusOK)
	require.Contains(t, body, `"user_id"`)

	// --- refresh: the refresh token must rotate (it's a server-stored
	// opaque handle; the old one is now revoked + replaced_by). The
	// access JWT is NOT asserted to differ — claims are second-precision
	// (iat/exp), so a refresh that lands in the same wall-clock second
	// as login produces a byte-identical signed JWT. That's fine: JWTs
	// are stateless in this design and the old access token was never
	// revoked anyway.
	rotated := httpRefresh(t, stack, tokens["refresh_token"].(string))
	require.NotEqual(t, tokens["refresh_token"], rotated["refresh_token"])
	require.NotEmpty(t, rotated["access_token"])

	// --- the rotated access token must work on a protected route.
	httpGetJSON(t, stack, "/users/"+user["id"].(string)+"/portfolio",
		rotated["access_token"].(string), http.StatusOK)

	// --- logout: revoke the latest refresh token
	httpLogout(t, stack, rotated["refresh_token"].(string))

	// --- a refresh attempt with the just-revoked token must fail
	httpRefreshExpect(t, stack, rotated["refresh_token"].(string), http.StatusUnauthorized)
}

func TestHTTPAuth_ReplayDetectionRevokesChain(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "replay")
	httpRegister(t, stack, email, "Replay User", "correct-horse-battery")
	tokens := httpLogin(t, stack, email, "correct-horse-battery")

	// First refresh — happy path. Old token is now revoked + replaced_by.
	rotated := httpRefresh(t, stack, tokens["refresh_token"].(string))

	// Second refresh with the SAME (now-revoked) original token = replay.
	// The server must return 401 AND revoke the entire chain reachable
	// from the original. We check the side-effect by trying to refresh
	// the rotated token afterward — it should also be 401.
	httpRefreshExpect(t, stack, tokens["refresh_token"].(string), http.StatusUnauthorized)

	httpRefreshExpect(t, stack, rotated["refresh_token"].(string), http.StatusUnauthorized)
}

// TestHTTPAuth_ConcurrentRefreshSerialisesAndDeniesLoser proves the
// transactional rotation in RefreshTokenRepository.Rotate: two refreshes
// of the SAME token fired in parallel must serialise — exactly one
// succeeds, the other is treated as replay. The losing goroutine
// receives 401 and the entire chain (old + winner's new) ends up
// revoked, so neither token can be used again.
//
// This is the regression test for the race called out in PR #3 review:
// without the row-locked tx, both goroutines could pass the freshness
// check, both insert new tokens, and the loser's new token would remain
// valid as an orphan. With Rotate, the second tx blocks on the FOR
// UPDATE lock, then sees revoked_at != NULL and bubbles
// ErrRefreshTokenRevoked, which the service maps to a 401 + family
// revoke.
func TestHTTPAuth_ConcurrentRefreshSerialisesAndDeniesLoser(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "concurrent")
	httpRegister(t, stack, email, "Race User", "correct-horse-battery")
	tokens := httpLogin(t, stack, email, "correct-horse-battery")
	original := tokens["refresh_token"].(string)

	type result struct {
		status int
		body   map[string]any
	}
	results := make(chan result, 2)
	start := make(chan struct{})

	for range 2 {
		go func() {
			<-start // fire both as close together as possible
			resp := httpPostJSON(t, stack, "/auth/refresh", map[string]string{
				"refresh_token": original,
			})
			defer resp.Body.Close()
			raw := mustReadString(t, resp)
			var body map[string]any
			_ = json.Unmarshal([]byte(raw), &body)
			results <- result{status: resp.StatusCode, body: body}
		}()
	}
	close(start)

	r1 := <-results
	r2 := <-results

	// Exactly one must succeed (200) and one must be denied (401). It
	// doesn't matter which goroutine wins — only that the outcomes are
	// asymmetric. If both got 200 the race is unfixed; if both got 401
	// the legitimate refresh failed.
	successes := 0
	failures := 0
	var winnerRefresh string
	for _, r := range []result{r1, r2} {
		switch r.status {
		case http.StatusOK:
			successes++
			if rt, ok := r.body["refresh_token"].(string); ok {
				winnerRefresh = rt
			}
		case http.StatusUnauthorized:
			failures++
		default:
			t.Fatalf("unexpected status from concurrent refresh: %d (body=%v)", r.status, r.body)
		}
	}
	require.Equal(t, 1, successes, "exactly one concurrent refresh must succeed")
	require.Equal(t, 1, failures, "exactly one concurrent refresh must be denied")
	require.NotEmpty(t, winnerRefresh)

	// Side-effect: the loser's denial revoked the family. The original
	// token is gone (winner already rotated it) and the winner's new
	// refresh token must now also be revoked, because the loser triggered
	// RevokeFamily(original).
	httpRefreshExpect(t, stack, original, http.StatusUnauthorized)
	httpRefreshExpect(t, stack, winnerRefresh, http.StatusUnauthorized)
}

func TestHTTPAuth_CrossAccountForbidden(t *testing.T) {
	stack := newAuthStack(t)
	emailA := uniqueEmail(t, "alice")
	emailB := uniqueEmail(t, "bob")
	userA := httpRegister(t, stack, emailA, "Alice", "alice-password-1")
	userB := httpRegister(t, stack, emailB, "Bob", "bob-password-1")
	tokensA := httpLogin(t, stack, emailA, "alice-password-1")

	// Alice tries to read Bob's portfolio.
	resp := httpGet(t, stack, "/users/"+userB["id"].(string)+"/portfolio",
		tokensA["access_token"].(string))
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Sanity: Alice CAN read her own.
	httpGetJSON(t, stack, "/users/"+userA["id"].(string)+"/portfolio",
		tokensA["access_token"].(string), http.StatusOK)
}

func TestHTTPAuth_LoginRejectsWrongPassword(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "wrongpw")
	httpRegister(t, stack, email, "User", "correct-horse-battery")

	resp := httpPostJSON(t, stack, "/auth/login", map[string]string{
		"email":    email,
		"password": "WRONG-password",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	wrongPwBody := mustReadString(t, resp)

	// Same flow with a never-registered email — response must look the
	// same as wrong-password (no account-enumeration oracle).
	resp2 := httpPostJSON(t, stack, "/auth/login", map[string]string{
		"email":    "ghost-" + email,
		"password": "anything",
	})
	defer resp2.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)
	require.Equal(t, wrongPwBody, mustReadString(t, resp2),
		"unknown email and wrong password must produce identical responses")
}

func TestHTTPAuth_ProtectedRequiresToken(t *testing.T) {
	stack := newAuthStack(t)
	resp := httpGet(t, stack, "/users/u1/portfolio", "" /* no token */)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// =============================================================================
// HTTP helpers — keep tests above readable
// =============================================================================

func httpRegister(t *testing.T, s *authStack, email, name, password string) map[string]any {
	t.Helper()
	resp := httpPostJSON(t, s, "/auth/register", map[string]string{
		"name": name, "email": email, "password": password,
	})
	defer resp.Body.Close()
	raw := mustReadString(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "register: body=%s", raw)
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	require.NotEmpty(t, body["id"])
	return body
}

func httpLogin(t *testing.T, s *authStack, email, password string) map[string]any {
	t.Helper()
	resp := httpPostJSON(t, s, "/auth/login", map[string]string{
		"email": email, "password": password,
	})
	defer resp.Body.Close()
	raw := mustReadString(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "login: body=%s", raw)
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	return body
}

func httpRefresh(t *testing.T, s *authStack, refreshToken string) map[string]any {
	t.Helper()
	resp := httpPostJSON(t, s, "/auth/refresh", map[string]string{
		"refresh_token": refreshToken,
	})
	defer resp.Body.Close()
	raw := mustReadString(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "refresh: body=%s", raw)
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	return body
}

func httpRefreshExpect(t *testing.T, s *authStack, refreshToken string, expectStatus int) {
	t.Helper()
	resp := httpPostJSON(t, s, "/auth/refresh", map[string]string{
		"refresh_token": refreshToken,
	})
	defer resp.Body.Close()
	require.Equal(t, expectStatus, resp.StatusCode, "refresh: body=%s", mustReadString(t, resp))
}

func httpLogout(t *testing.T, s *authStack, refreshToken string) {
	t.Helper()
	resp := httpPostJSON(t, s, "/auth/logout", map[string]string{
		"refresh_token": refreshToken,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func httpPostJSON(t *testing.T, s *authStack, path string, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	ctx, cancel := timeoutCtx(t)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.httpServer.URL+path, bytes.NewReader(buf))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func httpGet(t *testing.T, s *authStack, path, bearer string) *http.Response {
	t.Helper()
	ctx, cancel := timeoutCtx(t)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.httpServer.URL+path, nil)
	require.NoError(t, err)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func httpGetJSON(t *testing.T, s *authStack, path, bearer string, expectStatus int) string {
	t.Helper()
	resp := httpGet(t, s, path, bearer)
	defer resp.Body.Close()
	require.Equal(t, expectStatus, resp.StatusCode)
	return mustReadString(t, resp)
}

func mustReadString(t *testing.T, r *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return strings.TrimSpace(string(b))
}

// keep imports honest in case Bash strips one
var _ = context.Canceled
