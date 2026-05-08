package domain

import (
	"errors"
	"fmt"
)

// =============================================================================
// Sentinel Errors
// =============================================================================
//
// Sentinel errors are package-level variables that represent specific,
// well-known error conditions. They allow callers to check error identity
// using errors.Is, even through layers of wrapping.
//
// Convention: prefix with "Err" and use descriptive names.

var (
	// ErrUserNotFound indicates the requested user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrWalletNotFound indicates the requested wallet does not exist.
	ErrWalletNotFound = errors.New("wallet not found")

	// ErrInvalidWalletAddress indicates a malformed wallet address.
	ErrInvalidWalletAddress = errors.New("invalid wallet address")

	// ErrUnsupportedNetwork indicates no provider exists for the blockchain network.
	ErrUnsupportedNetwork = errors.New("unsupported blockchain network")

	// ErrUnsupportedAsset indicates the asset is not supported by the provider.
	ErrUnsupportedAsset = errors.New("unsupported asset")

	// ErrProviderUnavailable indicates the external provider is down or unreachable.
	ErrProviderUnavailable = errors.New("provider unavailable")

	// ErrUpstreamTimeout indicates an external call exceeded its deadline.
	ErrUpstreamTimeout = errors.New("upstream timeout")

	// ErrInvalidCredentials indicates the supplied email/password pair did
	// not match an active account. Deliberately collapses "no such email"
	// and "wrong password" into one error so an attacker cannot enumerate
	// accounts by observing different responses.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrUserAlreadyExists indicates a registration attempt for an email
	// that is already in use (case-insensitive).
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrUnauthenticated indicates the caller did not present a valid
	// access token. Maps to HTTP 401 / gRPC Unauthenticated.
	ErrUnauthenticated = errors.New("unauthenticated")

	// ErrForbidden indicates the caller is authenticated but not allowed
	// to access this specific resource. Maps to HTTP 403 / gRPC PermissionDenied.
	ErrForbidden = errors.New("forbidden")

	// ErrRefreshTokenNotFound indicates the presented refresh token does
	// not exist in the store. Treated as authentication failure by callers.
	ErrRefreshTokenNotFound = errors.New("refresh token not found")

	// ErrRefreshTokenRevoked indicates the token was previously revoked
	// (either explicitly via logout or implicitly via rotation). The
	// service layer interprets this as a replay attempt and revokes the
	// entire chain on first detection.
	ErrRefreshTokenRevoked = errors.New("refresh token revoked")

	// ErrRefreshTokenExpired indicates the token's expires_at has passed.
	ErrRefreshTokenExpired = errors.New("refresh token expired")
)

// =============================================================================
// Error Codes
// =============================================================================
//
// Error codes are machine-readable identifiers included in API responses.
// They decouple the HTTP layer from domain logic: a handler maps a code
// to an HTTP status, while the code itself is domain-driven.

type ErrorCode string

const (
	CodeNotFound           ErrorCode = "not_found"
	CodeValidation         ErrorCode = "validation_error"
	CodeProviderFailure    ErrorCode = "provider_failure"
	CodeUpstreamTimeout    ErrorCode = "upstream_timeout"
	CodeInternal           ErrorCode = "internal_error"
	CodeInvalidInput       ErrorCode = "invalid_input"
	CodeUnsupportedNetwork ErrorCode = "unsupported_network"
	CodeUnauthenticated    ErrorCode = "unauthenticated"
	CodeForbidden          ErrorCode = "forbidden"
	CodeConflict           ErrorCode = "conflict"
)

// =============================================================================
// Structured Error: AppError
// =============================================================================
//
// AppError is the project's structured error type. It carries:
//   - Code:      machine-readable error code (for HTTP mapping)
//   - Message:   human-readable description (safe for API responses)
//   - Op:        the operation that failed (for internal logging)
//   - Err:       the underlying cause (may contain sensitive details)
//
// AppError implements both error and Unwrap interfaces, which means:
//   - errors.Is(appErr, someSentinel)  works through the chain
//   - errors.As(err, &appErr)          extracts AppError from any depth
//
// IMPORTANT: The Message field is what gets sent to the client.
// The Err field is logged internally but NEVER exposed in API responses.

type AppError struct {
	Code    ErrorCode
	Message string
	Op      string // operation context, e.g. "UserRepository.FindByID"
	Err     error  // underlying cause (may be nil)
}

// Error implements the error interface.
// It builds a human-readable chain: "operation: message: cause".
func (e *AppError) Error() string {
	if e.Err != nil {
		if e.Op != "" {
			return fmt.Sprintf("%s: %s: %v", e.Op, e.Message, e.Err)
		}
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	if e.Op != "" {
		return fmt.Sprintf("%s: %s", e.Op, e.Message)
	}
	return e.Message
}

// Unwrap returns the underlying error, enabling errors.Is and errors.As
// to traverse the error chain.
//
// Without Unwrap, wrapping would break sentinel checks:
//
//	appErr := &AppError{Err: ErrUserNotFound}
//	errors.Is(appErr, ErrUserNotFound) // true, because Unwrap exposes the chain
func (e *AppError) Unwrap() error {
	return e.Err
}

// =============================================================================
// Constructor Helpers
// =============================================================================
//
// These helpers keep error creation consistent across the codebase.
// Each layer (repo, service, provider) uses them to produce well-structured errors.

// NewNotFoundError creates an AppError for missing resources.
func NewNotFoundError(op string, err error) *AppError {
	return &AppError{
		Code:    CodeNotFound,
		Message: "resource not found",
		Op:      op,
		Err:     err,
	}
}

// NewValidationError creates an AppError for invalid input.
func NewValidationError(op, message string) *AppError {
	return &AppError{
		Code:    CodeValidation,
		Message: message,
		Op:      op,
	}
}

// NewProviderError creates an AppError for external provider failures.
func NewProviderError(op string, err error) *AppError {
	return &AppError{
		Code:    CodeProviderFailure,
		Message: "external provider failed",
		Op:      op,
		Err:     err,
	}
}

// NewTimeoutError creates an AppError for upstream timeouts.
func NewTimeoutError(op string, err error) *AppError {
	return &AppError{
		Code:    CodeUpstreamTimeout,
		Message: "request timed out",
		Op:      op,
		Err:     err,
	}
}

// NewInternalError creates an AppError for unexpected internal failures.
// The message is kept generic to avoid leaking implementation details.
func NewInternalError(op string, err error) *AppError {
	return &AppError{
		Code:    CodeInternal,
		Message: "internal server error",
		Op:      op,
		Err:     err,
	}
}

// NewUnauthenticatedError creates an AppError for missing/invalid auth credentials.
// The message stays vague on purpose — it must not leak whether the email
// existed or the password was wrong.
func NewUnauthenticatedError(op string, err error) *AppError {
	return &AppError{
		Code:    CodeUnauthenticated,
		Message: "authentication required",
		Op:      op,
		Err:     err,
	}
}

// NewForbiddenError creates an AppError for authorised-but-not-allowed cases,
// e.g., a user tries to read another user's portfolio.
func NewForbiddenError(op, message string) *AppError {
	if message == "" {
		message = "access denied"
	}
	return &AppError{
		Code:    CodeForbidden,
		Message: message,
		Op:      op,
	}
}

// NewConflictError creates an AppError for resource conflicts, e.g., a
// registration attempt with an email that is already in use.
func NewConflictError(op, message string, err error) *AppError {
	return &AppError{
		Code:    CodeConflict,
		Message: message,
		Op:      op,
		Err:     err,
	}
}
