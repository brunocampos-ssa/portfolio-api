// restclient — end-to-end demo of the REST authentication flow.
//
// What it does:
//   1. Register a fresh demo user (idempotent — safe to re-run).
//   2. Log in to get an access + refresh token pair.
//   3. Call a protected endpoint (GET /users/{id}/portfolio) with the
//      access token. Show the response.
//   4. Refresh — exchange the refresh token for a new pair.
//   5. Call the protected endpoint again with the NEW access token.
//   6. Logout — revoke the latest refresh token.
//
// Run against a local server:
//
//   make run        # in another terminal
//   go run ./examples/restclient
//
// Override the base URL:
//
//   API_BASE=https://staging.example.com go run ./examples/restclient
//
// The client deliberately uses ONLY the standard library — students
// should not have to install anything new to play with the API.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	demoEmail    = "alice-restclient@example.com"
	demoName     = "Alice (REST client demo)"
	demoPassword = "correct-horse-battery-staple"
)

type registerResp struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type tokensResp struct {
	AccessToken           string    `json:"access_token"`
	AccessTokenExpiresAt  time.Time `json:"access_token_expires_at"`
	RefreshToken          string    `json:"refresh_token"`
	RefreshTokenExpiresAt time.Time `json:"refresh_token_expires_at"`
	TokenType             string    `json:"token_type"`
}

func main() {
	base := envOr("API_BASE", "http://localhost:8080")
	cli := &http.Client{Timeout: 10 * time.Second}

	log.Printf("==> Using base URL: %s", base)

	// 1. Register. If the user already exists from a previous run, the
	// API returns 409 — that's fine, we just continue to login.
	userID, err := register(cli, base)
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("    registered or already-existed: id=%s email=%s", userID, demoEmail)

	// 2. Login.
	tokens, err := login(cli, base)
	if err != nil {
		log.Fatalf("login: %v", err)
	}
	log.Printf("==> login OK")
	log.Printf("    access_token expires at  %s", tokens.AccessTokenExpiresAt.Format(time.RFC3339))
	log.Printf("    refresh_token expires at %s", tokens.RefreshTokenExpiresAt.Format(time.RFC3339))

	// 3. Call protected endpoint with the access token.
	log.Println("==> GET /users/{id}/portfolio (with access token)")
	if err := getPortfolio(cli, base, tokens.AccessToken, userIDFromLogin(tokens, userID)); err != nil {
		log.Fatalf("get portfolio: %v", err)
	}

	// 4. Refresh the token pair.
	rotated, err := refresh(cli, base, tokens.RefreshToken)
	if err != nil {
		log.Fatalf("refresh: %v", err)
	}
	log.Printf("==> refresh OK — got new pair (rotation chain advanced)")
	log.Printf("    old refresh token is now revoked")

	// 5. Hit the protected endpoint with the NEW access token.
	log.Println("==> GET /users/{id}/portfolio (with rotated access token)")
	if err := getPortfolio(cli, base, rotated.AccessToken, userIDFromLogin(tokens, userID)); err != nil {
		log.Fatalf("get portfolio (rotated): %v", err)
	}

	// 6. Logout — revoke the latest refresh token. Subsequent refresh
	// attempts with that token would now be rejected.
	if err := logout(cli, base, rotated.RefreshToken); err != nil {
		log.Fatalf("logout: %v", err)
	}
	log.Println("==> logout OK")

	log.Println("Done. Try replaying the rotated.RefreshToken — the server should detect replay and revoke the chain.")
}

// =============================================================================
// HTTP helpers
// =============================================================================

func register(cli *http.Client, base string) (string, error) {
	body := map[string]string{
		"name":     demoName,
		"email":    demoEmail,
		"password": demoPassword,
	}
	resp, err := postJSON(cli, base+"/auth/register", body, "")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var r registerResp
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return "", fmt.Errorf("decode register response: %w", err)
		}
		return r.ID, nil
	case http.StatusConflict:
		// Already exists — recover the user id by hitting login.
		return "", nil
	default:
		return "", fmt.Errorf("register: unexpected status %d: %s", resp.StatusCode, readBody(resp))
	}
}

func login(cli *http.Client, base string) (*tokensResp, error) {
	body := map[string]string{
		"email":    demoEmail,
		"password": demoPassword,
	}
	resp, err := postJSON(cli, base+"/auth/login", body, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("login: status %d: %s", resp.StatusCode, readBody(resp))
	}
	var t tokensResp
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, fmt.Errorf("decode login response: %w", err)
	}
	return &t, nil
}

func refresh(cli *http.Client, base, refreshToken string) (*tokensResp, error) {
	body := map[string]string{"refresh_token": refreshToken}
	resp, err := postJSON(cli, base+"/auth/refresh", body, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh: status %d: %s", resp.StatusCode, readBody(resp))
	}
	var t tokensResp
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, fmt.Errorf("decode refresh response: %w", err)
	}
	return &t, nil
}

func logout(cli *http.Client, base, refreshToken string) error {
	body := map[string]string{"refresh_token": refreshToken}
	resp, err := postJSON(cli, base+"/auth/logout", body, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("logout: status %d: %s", resp.StatusCode, readBody(resp))
	}
	return nil
}

func getPortfolio(cli *http.Client, base, accessToken, userID string) error {
	url := fmt.Sprintf("%s/users/%s/portfolio", base, userID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, readBody(resp))
	}
	// Pretty-print JSON.
	var pretty bytes.Buffer
	if _, err := io.Copy(&pretty, resp.Body); err != nil {
		return err
	}
	log.Printf("    response: %s", pretty.String())
	return nil
}

// =============================================================================
// helpers
// =============================================================================

func postJSON(cli *http.Client, url string, body any, bearer string) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return cli.Do(req)
}

func readBody(resp *http.Response) string {
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// userIDFromLogin works around the fact that the login response does not
// include the user id (only tokens). We recover it from the JWT subject
// without verifying the signature — fine for a demo, but production
// clients should obey the access-token contract and never decode it.
func userIDFromLogin(t *tokensResp, fallback string) string {
	if fallback != "" {
		return fallback
	}
	// Best-effort: decode the JWT payload for the demo. Real clients
	// would have stored the user id alongside the tokens at login.
	subject := jwtSubjectInsecure(t.AccessToken)
	if subject != "" {
		return subject
	}
	return "unknown"
}

// jwtSubjectInsecure decodes the JWT payload WITHOUT verifying the
// signature and returns the "sub" claim. The client trusts the server
// that just issued the token; a real client should also store the user
// id alongside the tokens at login.
func jwtSubjectInsecure(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var c struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return ""
	}
	return c.Sub
}
