package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/httpapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

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

	req := httptest.NewRequest(http.MethodGet, "/users/u1/portfolio", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/users/unknown/portfolio", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/users/unknown/wallets", nil)
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

	req := httptest.NewRequest(http.MethodPost, "/users/u1/wallets", strings.NewReader("not json"))
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
	req := httptest.NewRequest(http.MethodPost, "/users/u1/wallets", strings.NewReader(body))
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
	req := httptest.NewRequest(http.MethodPost, "/users/u1/wallets", strings.NewReader(body))
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
