package domain_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Test: Error Wrapping with %w
// =============================================================================

func TestAppError_ErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      *domain.AppError
		contains string
	}{
		{
			name: "with operation and cause",
			err: &domain.AppError{
				Code:    domain.CodeNotFound,
				Message: "user not found",
				Op:      "UserRepo.FindByID",
				Err:     fmt.Errorf("sql: no rows"),
			},
			contains: "UserRepo.FindByID: user not found: sql: no rows",
		},
		{
			name: "with cause only",
			err: &domain.AppError{
				Code:    domain.CodeInternal,
				Message: "database error",
				Err:     fmt.Errorf("connection refused"),
			},
			contains: "database error: connection refused",
		},
		{
			name: "message only",
			err: &domain.AppError{
				Code:    domain.CodeValidation,
				Message: "invalid input",
			},
			contains: "invalid input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.contains {
				t.Errorf("Error() = %q, want %q", got, tt.contains)
			}
		})
	}
}

// =============================================================================
// Test: errors.Is — Sentinel Error Detection Through Wrapping
// =============================================================================
//
// This test proves that errors.Is traverses the error chain.
// Even when ErrUserNotFound is wrapped multiple times, errors.Is finds it.

func TestErrorsIs_SentinelDetection(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{
			name:   "direct sentinel",
			err:    domain.ErrUserNotFound,
			target: domain.ErrUserNotFound,
			want:   true,
		},
		{
			name:   "wrapped once with fmt.Errorf",
			err:    fmt.Errorf("repo: %w", domain.ErrUserNotFound),
			target: domain.ErrUserNotFound,
			want:   true,
		},
		{
			name:   "wrapped twice",
			err:    fmt.Errorf("service: %w", fmt.Errorf("repo: %w", domain.ErrUserNotFound)),
			target: domain.ErrUserNotFound,
			want:   true,
		},
		{
			name: "wrapped in AppError",
			err: &domain.AppError{
				Code:    domain.CodeNotFound,
				Message: "not found",
				Err:     domain.ErrUserNotFound,
			},
			target: domain.ErrUserNotFound,
			want:   true,
		},
		{
			name: "AppError wrapping fmt.Errorf wrapping sentinel",
			err: &domain.AppError{
				Code:    domain.CodeNotFound,
				Message: "not found",
				Op:      "service.GetUser",
				Err:     fmt.Errorf("repo: %w", domain.ErrUserNotFound),
			},
			target: domain.ErrUserNotFound,
			want:   true,
		},
		{
			name:   "different sentinel",
			err:    domain.ErrWalletNotFound,
			target: domain.ErrUserNotFound,
			want:   false,
		},
		{
			name:   "unrelated error",
			err:    fmt.Errorf("something else"),
			target: domain.ErrUserNotFound,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errors.Is(tt.err, tt.target)
			if got != tt.want {
				t.Errorf("errors.Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// Test: errors.As — Structured Error Extraction
// =============================================================================
//
// This test proves that errors.As can extract an *AppError from any depth
// in the error chain, giving access to Code, Message, Op, etc.

func TestErrorsAs_AppErrorExtraction(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode domain.ErrorCode
		wantOk   bool
	}{
		{
			name: "direct AppError",
			err: &domain.AppError{
				Code:    domain.CodeNotFound,
				Message: "user not found",
			},
			wantCode: domain.CodeNotFound,
			wantOk:   true,
		},
		{
			name: "AppError wrapped with fmt.Errorf",
			err: fmt.Errorf("handler: %w", &domain.AppError{
				Code:    domain.CodeValidation,
				Message: "bad input",
			}),
			wantCode: domain.CodeValidation,
			wantOk:   true,
		},
		{
			name:   "plain error — not an AppError",
			err:    fmt.Errorf("plain error"),
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var appErr *domain.AppError
			ok := errors.As(tt.err, &appErr)

			if ok != tt.wantOk {
				t.Fatalf("errors.As() ok = %v, want %v", ok, tt.wantOk)
			}
			if ok && appErr.Code != tt.wantCode {
				t.Errorf("AppError.Code = %q, want %q", appErr.Code, tt.wantCode)
			}
		})
	}
}

// =============================================================================
// Test: Unwrap Chain
// =============================================================================

func TestAppError_Unwrap(t *testing.T) {
	inner := fmt.Errorf("connection refused")
	appErr := &domain.AppError{
		Code:    domain.CodeInternal,
		Message: "db error",
		Err:     inner,
	}

	unwrapped := appErr.Unwrap()
	if unwrapped != inner {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestAppError_UnwrapNil(t *testing.T) {
	appErr := &domain.AppError{
		Code:    domain.CodeValidation,
		Message: "bad input",
	}

	if appErr.Unwrap() != nil {
		t.Error("Unwrap() should return nil when Err is nil")
	}
}

// =============================================================================
// Test: Constructor Helpers
// =============================================================================

func TestNewNotFoundError(t *testing.T) {
	err := domain.NewNotFoundError("test.Op", domain.ErrUserNotFound)

	if err.Code != domain.CodeNotFound {
		t.Errorf("Code = %q, want %q", err.Code, domain.CodeNotFound)
	}
	if !errors.Is(err, domain.ErrUserNotFound) {
		t.Error("expected errors.Is to find ErrUserNotFound")
	}
}

func TestNewProviderError(t *testing.T) {
	cause := fmt.Errorf("timeout")
	err := domain.NewProviderError("eth.GetBalance", cause)

	if err.Code != domain.CodeProviderFailure {
		t.Errorf("Code = %q, want %q", err.Code, domain.CodeProviderFailure)
	}

	var appErr *domain.AppError
	if !errors.As(err, &appErr) {
		t.Fatal("expected errors.As to extract AppError")
	}
	if appErr.Op != "eth.GetBalance" {
		t.Errorf("Op = %q, want %q", appErr.Op, "eth.GetBalance")
	}
}
