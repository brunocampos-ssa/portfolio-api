package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/httpapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// authedReq builds an httptest request with auth claims attached, as if
// the JWT middleware had already verified a token whose subject is the
// given userID. Tests that exercise the protected /users/{id} endpoints
// must use this helper so requireSelf passes.
func authedReq(method, path, subject string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, path, body)
	req = req.WithContext(auth.WithClaims(req.Context(), &auth.Claims{
		Subject:   subject,
		IssuedAt:  time.Now().Add(-time.Minute),
		ExpiresAt: time.Now().Add(time.Hour),
	}))
	return req
}

// =============================================================================
// Mock implementations for handler tests
// =============================================================================

type mockUserRepo struct {
	users map[string]*domain.User
}

func (m *mockUserRepo) FindByID(_ context.Context, id string) (*domain.User, error) {
	user, ok := m.users[id]
	if !ok {
		return nil, fmt.Errorf("UserRepository.FindByID: %w", domain.ErrUserNotFound)
	}
	return user, nil
}

// Stubs added in Module 4 — handler tests don't exercise these paths.
func (m *mockUserRepo) FindByEmail(_ context.Context, _ string) (*domain.User, error) {
	return nil, fmt.Errorf("UserRepository.FindByEmail: %w", domain.ErrUserNotFound)
}

func (m *mockUserRepo) Create(_ context.Context, _ *domain.User, _ string) error {
	return fmt.Errorf("not implemented in this mock")
}

type mockWalletRepo struct {
	wallets map[string][]domain.Wallet
}

func (m *mockWalletRepo) FindByUserID(_ context.Context, userID string) ([]domain.Wallet, error) {
	return m.wallets[userID], nil
}

func (m *mockWalletRepo) FindByBlockchain(_ context.Context, blockchain string) ([]domain.Wallet, error) {
	return nil, nil
}

func (m *mockWalletRepo) Create(_ context.Context, w *domain.Wallet) error {
	return nil
}

type mockBalanceProvider struct {
	network string
	balance float64
	asset   string
}

func (m *mockBalanceProvider) Network() string { return m.network }
func (m *mockBalanceProvider) GetBalance(_ context.Context, _ string) (float64, string, error) {
	return m.balance, m.asset, nil
}

type mockPriceProvider struct {
	prices map[string]float64
}

func (m *mockPriceProvider) GetPriceUSD(_ context.Context, asset string) (float64, error) {
	price, ok := m.prices[asset]
	if !ok {
		return 0, fmt.Errorf("unknown asset: %s", asset)
	}
	return price, nil
}

func newTestHandler() *httpapi.Handler {
	registry := blockchain.NewProviderRegistry()
	registry.Register(&mockBalanceProvider{network: "ethereum", balance: 1.0, asset: "ETH"})

	svc := service.NewPortfolioService(
		&mockUserRepo{users: map[string]*domain.User{
			"u1": {ID: "u1", Name: "Alice"},
		}},
		&mockWalletRepo{wallets: map[string][]domain.Wallet{
			"u1": {{ID: "w1", UserID: "u1", Blockchain: "ethereum", Address: "0xABC"}},
		}},
		registry,
		&mockPriceProvider{prices: map[string]float64{"ETH": 3000.0}},
	)

	return httpapi.NewHandler(svc)
}

// =============================================================================
// Test: Handler Error → HTTP Status Code Mapping
// =============================================================================
//
// These tests verify the complete flow:
//   repository error → service translation → handler HTTP mapping

func TestHandleGetPortfolio_Success(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := authedReq(http.MethodGet, "/users/u1/portfolio", "u1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var portfolio domain.Portfolio
	if err := json.NewDecoder(rec.Body).Decode(&portfolio); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if portfolio.TotalUSD != 3000.0 {
		t.Errorf("TotalUSD = %f, want 3000.0", portfolio.TotalUSD)
	}
}

func TestHandleGetPortfolio_UserNotFound_Returns404(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := authedReq(http.MethodGet, "/users/unknown/portfolio", "unknown", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	// Verify the response body contains the error code, not raw SQL details.
	var apiErr httpapi.APIError
	if err := json.NewDecoder(rec.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode error response: %v", err)
	}

	if apiErr.Error.Code != "not_found" {
		t.Errorf("error code = %q, want %q", apiErr.Error.Code, "not_found")
	}
}

func TestHandleGetWallets_UserNotFound_Returns404(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := authedReq(http.MethodGet, "/users/unknown/wallets", "unknown", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAddWallet_InvalidJSON_Returns400(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := authedReq(http.MethodPost, "/users/u1/wallets", "u1", strings.NewReader("not json"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleAddWallet_EmptyFields_Returns400(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"blockchain": "", "address": ""}`
	req := authedReq(http.MethodPost, "/users/u1/wallets", "u1", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleAddWallet_InvalidAddress_Returns400(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"blockchain": "ethereum", "address": "not-a-valid-address"}`
	req := authedReq(http.MethodPost, "/users/u1/wallets", "u1", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleHealth(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// =============================================================================
// Module 4 — requireSelf authorisation
// =============================================================================

// TestProtectedRoute_ForbidsAccessToOtherUser proves the cross-account
// guard: a token whose subject is "u1" cannot read /users/u2/portfolio.
func TestProtectedRoute_ForbidsAccessToOtherUser(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := authedReq(http.MethodGet, "/users/u2/portfolio", "u1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}

	var apiErr httpapi.APIError
	if err := json.NewDecoder(rec.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr.Error.Code != "forbidden" {
		t.Errorf("code = %q, want %q", apiErr.Error.Code, "forbidden")
	}
}

// TestProtectedRoute_RejectsMissingClaims simulates the wiring-bug case
// where someone forgot to put the JWT middleware in front of /users/...
// The handler must NOT silently pass through; it must return 401.
func TestProtectedRoute_RejectsMissingClaims(t *testing.T) {
	handler := newTestHandler()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// httptest.NewRequest, NOT authedReq — no claims on context.
	req := httptest.NewRequest(http.MethodGet, "/users/u1/portfolio", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
