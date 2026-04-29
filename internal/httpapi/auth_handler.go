package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// =============================================================================
// AuthHandler — REST surface for /auth/*
// =============================================================================
//
// Four endpoints:
//
//   POST /auth/register   {name, email, password}                → 201 + user
//   POST /auth/login      {email, password}                      → 200 + tokens
//   POST /auth/refresh    {refresh_token}                        → 200 + tokens
//   POST /auth/logout     {refresh_token}                        → 204
//
// Cookies are intentionally NOT used. Mobile clients and the gRPC mirror
// both want explicit token handling, so the response body is the single
// channel for delivering tokens. The trade-off is that the browser-based
// client must store tokens carefully — README.pt-BR Aula 1 walks through
// the relevant XSS-vs-CSRF discussion.

// AuthHandler exposes the registration, login, refresh, and logout endpoints.
type AuthHandler struct {
	auth *service.AuthService
}

// NewAuthHandler wires the auth REST handler. Defensive nil check matches
// the rest of the project's constructor style.
func NewAuthHandler(auth *service.AuthService) *AuthHandler {
	if auth == nil {
		panic("httpapi.NewAuthHandler: auth must not be nil")
	}
	return &AuthHandler{auth: auth}
}

// RegisterRoutes attaches /auth/* to the given mux. Public — these
// endpoints must NOT be behind the JWT middleware.
func (h *AuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/register", h.HandleRegister)
	mux.HandleFunc("POST /auth/login", h.HandleLogin)
	mux.HandleFunc("POST /auth/refresh", h.HandleRefresh)
	mux.HandleFunc("POST /auth/logout", h.HandleLogout)
}

// =============================================================================
// request/response shapes
// =============================================================================

type registerRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type registerResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	AccessToken           string    `json:"access_token"`
	AccessTokenExpiresAt  time.Time `json:"access_token_expires_at"`
	RefreshToken          string    `json:"refresh_token"`
	RefreshTokenExpiresAt time.Time `json:"refresh_token_expires_at"`
	TokenType             string    `json:"token_type"` // always "Bearer"
}

// =============================================================================
// handlers
// =============================================================================

// HandleRegister creates a new account.
//
// Successful response is the user record (no token). Clients call /login
// next to get a token pair.
func (h *AuthHandler) HandleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "invalid JSON body"},
		})
		return
	}

	user, err := h.auth.Register(r.Context(), service.RegisterInput{
		Name:     req.Name,
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, registerResponse{
		ID:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: user.CreatedAt,
	})
}

// HandleLogin authenticates and returns an access + refresh pair.
func (h *AuthHandler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "invalid JSON body"},
		})
		return
	}

	tokens, err := h.auth.Login(r.Context(), service.LoginInput{
		Email:     req.Email,
		Password:  req.Password,
		UserAgent: r.UserAgent(),
		IP:        clientIP(r),
	})
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, tokenResponseFrom(tokens))
}

// HandleRefresh exchanges a valid refresh token for a fresh pair.
//
// The same response shape as Login — clients that re-use the login parser
// don't need a special case.
func (h *AuthHandler) HandleRefresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "invalid JSON body"},
		})
		return
	}

	tokens, err := h.auth.Refresh(r.Context(), req.RefreshToken, r.UserAgent(), clientIP(r))
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, tokenResponseFrom(tokens))
}

// HandleLogout revokes the presented refresh token.
//
// Returns 204 No Content on success. Service.Logout is idempotent —
// unknown tokens are silently accepted to deny the attacker an oracle.
func (h *AuthHandler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIError{
			Error: ErrorBody{Code: "invalid_input", Message: "invalid JSON body"},
		})
		return
	}

	if err := h.auth.Logout(r.Context(), req.RefreshToken); err != nil {
		writeError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// =============================================================================
// helpers
// =============================================================================

func tokenResponseFrom(t *service.Tokens) tokenResponse {
	return tokenResponse{
		AccessToken:           t.AccessToken,
		AccessTokenExpiresAt:  t.AccessTokenExpiresAt,
		RefreshToken:          t.RefreshToken,
		RefreshTokenExpiresAt: t.RefreshTokenExpiresAt,
		TokenType:             "Bearer",
	}
}

// clientIP extracts a best-effort client IP for the refresh-token audit
// trail. We honour X-Forwarded-For (first hop) when present — typical
// behind a load balancer — and fall back to RemoteAddr otherwise.
//
// This is NOT used for any authorisation decision. It exists only so the
// refresh_tokens table records WHERE a session was issued. Spoofed values
// are harmless to the auth flow.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if comma := strings.IndexByte(xff, ','); comma > 0 {
			return strings.TrimSpace(xff[:comma])
		}
		return strings.TrimSpace(xff)
	}
	addr := r.RemoteAddr
	if colon := strings.LastIndexByte(addr, ':'); colon > 0 {
		return addr[:colon]
	}
	return addr
}
