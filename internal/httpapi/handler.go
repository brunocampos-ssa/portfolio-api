package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// Handler handles all HTTP endpoints for the portfolio API.
type Handler struct {
	service *service.PortfolioService
}

// NewHandler creates a handler with the given service.
//
// Defensive programming: panic if service is nil.
func NewHandler(svc *service.PortfolioService) *Handler {
	if svc == nil {
		panic("httpapi.NewHandler: service must not be nil")
	}
	return &Handler{service: svc}
}

// RegisterRoutes sets up all HTTP routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.HandleHealth)
	mux.HandleFunc("GET /users/{id}/portfolio", h.HandleGetPortfolio)
	mux.HandleFunc("GET /users/{id}/wallets", h.HandleGetWallets)
	mux.HandleFunc("POST /users/{id}/wallets", h.HandleAddWallet)

	// Educational/debug endpoints — intentionally trigger panics
	// to demonstrate recovery middleware. NEVER include these in production.
	mux.HandleFunc("GET /debug/panic", h.HandleDebugPanic)
	mux.HandleFunc("GET /debug/panic/nilmap", h.HandleDebugPanicNilMap)
}

// HandleHealth returns a simple health check response.
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleGetPortfolio handles GET /users/{id}/portfolio
//
// Error flow:
//  1. Extract user ID from path
//  2. Call service.GetPortfolio
//  3. If error → writeError maps domain error to HTTP status
//  4. If success → write portfolio as JSON
func (h *Handler) HandleGetPortfolio(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "user id is required"},
		})
		return
	}

	portfolio, err := h.service.GetPortfolio(r.Context(), userID)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, portfolio)
}

// HandleGetWallets handles GET /users/{id}/wallets
func (h *Handler) HandleGetWallets(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "user id is required"},
		})
		return
	}

	wallets, err := h.service.GetWallets(r.Context(), userID)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wallets)
}

// addWalletRequest is the expected JSON body for POST /users/{id}/wallets.
type addWalletRequest struct {
	Blockchain string `json:"blockchain"`
	Address    string `json:"address"`
}

// HandleAddWallet handles POST /users/{id}/wallets
//
// Demonstrates:
//   - Input validation (empty fields, JSON decode errors)
//   - Domain validation (invalid address format, unsupported blockchain)
//   - Proper error translation to HTTP responses
func (h *Handler) HandleAddWallet(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "user id is required"},
		})
		return
	}

	var req addWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "invalid JSON body"},
		})
		return
	}

	// Validate required fields.
	req.Blockchain = strings.TrimSpace(strings.ToLower(req.Blockchain))
	req.Address = strings.TrimSpace(req.Address)

	if req.Blockchain == "" || req.Address == "" {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "validation_error", Message: "blockchain and address are required"},
		})
		return
	}

	wallet, err := h.service.AddWallet(r.Context(), userID, req.Blockchain, req.Address)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, wallet)
}

// HandleDebugPanic intentionally panics to demonstrate recovery middleware.
//
// THIS IS FOR EDUCATIONAL PURPOSES ONLY.
// In a real application, you would never have an endpoint that panics.
//
// How to test:
//
//	curl http://localhost:8080/debug/panic
//
// Expected behavior:
//   - Recovery middleware catches the panic
//   - Returns HTTP 500 with a safe JSON error
//   - Server continues running (other endpoints still work)
//   - Full stack trace is logged server-side
func (h *Handler) HandleDebugPanic(w http.ResponseWriter, r *http.Request) {
	log.Println("DEBUG: about to panic intentionally...")

	// Simulate a programmer error / invariant violation.
	// In real code, this could be a nil map access, index out of range,
	// or an explicit panic("unreachable") in a code path that should
	// never execute.
	panic("intentional panic for educational demonstration")
}

// HandleDebugPanicNilMap is an alternative panic demo that simulates
// a nil map access — a very common source of panics in Go.
func (h *Handler) HandleDebugPanicNilMap(w http.ResponseWriter, r *http.Request) {
	var m map[string]string
	// This will panic: assignment to entry in nil map
	m["key"] = "value"
	writeJSON(w, http.StatusOK, m)
}

// validateNotEmpty is a small helper for input validation.
// It returns a domain.AppError if the value is empty.
func validateNotEmpty(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return &domain.AppError{
			Code:    domain.CodeInvalidInput,
			Message: field + " is required",
		}
	}
	return nil
}
