package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// APIError is the JSON structure returned to API clients on error.
//
// IMPORTANT: this only contains safe, client-facing information.
// Internal details (stack traces, SQL errors, provider URLs) are logged
// server-side but NEVER included in the response.
type APIError struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody contains the machine-readable code and human-readable message.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON encodes data as JSON and writes it to the response.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("ERROR: encode json response: %v", err)
	}
}

// writeError maps a domain/application error to the appropriate HTTP
// status code and writes a safe JSON error response.
//
// This is where ERROR TRANSLATION happens at the HTTP boundary:
//
//	domain.AppError.Code → HTTP status code
//	domain.AppError.Message → client-facing message
//	domain.AppError.Err → logged internally, never sent to client
//
// The mapping uses errors.As to extract AppError from any depth in the
// error chain. If the error is not an AppError, we fall back to 500.
func writeError(w http.ResponseWriter, err error) {
	var appErr *domain.AppError

	if errors.As(err, &appErr) {
		status := mapCodeToHTTPStatus(appErr.Code)

		// Log the full error chain internally for debugging.
		log.Printf("ERROR [%s] %s: %v", appErr.Code, appErr.Op, appErr.Err)

		writeJSON(w, status, APIError{
			Error: ErrorBody{
				Code:    string(appErr.Code),
				Message: appErr.Message,
			},
		})
		return
	}

	// Fallback: unknown error type → 500 with generic message.
	// NEVER expose err.Error() to the client — it may contain SQL, URLs, etc.
	log.Printf("ERROR [untyped]: %v", err)
	writeJSON(w, http.StatusInternalServerError, APIError{
		Error: ErrorBody{
			Code:    string(domain.CodeInternal),
			Message: "internal server error",
		},
	})
}

// mapCodeToHTTPStatus converts a domain error code to an HTTP status.
//
// This is the single source of truth for error → status mapping.
// Adding a new domain error code? Add a case here.
func mapCodeToHTTPStatus(code domain.ErrorCode) int {
	switch code {
	case domain.CodeNotFound:
		return http.StatusNotFound // 404
	case domain.CodeValidation, domain.CodeInvalidInput:
		return http.StatusBadRequest // 400
	case domain.CodeUnsupportedNetwork:
		return http.StatusBadRequest // 400
	case domain.CodeProviderFailure:
		return http.StatusBadGateway // 502
	case domain.CodeUpstreamTimeout:
		return http.StatusGatewayTimeout // 504
	case domain.CodeInternal:
		return http.StatusInternalServerError // 500
	default:
		return http.StatusInternalServerError // 500
	}
}
