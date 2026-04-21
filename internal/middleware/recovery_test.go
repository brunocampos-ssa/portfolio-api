package middleware_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
)

// =============================================================================
// Test: Recovery Middleware Catches Panics
// =============================================================================
//
// This is one of the most important tests in the project.
// It proves that:
//  1. A panic in a handler does NOT crash the server
//  2. The client gets a safe 500 JSON response
//  3. Internal panic details are NOT leaked to the client

func TestRecovery_CatchesPanic(t *testing.T) {
	// Handler that panics.
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went terribly wrong")
	})

	// Wrap with recovery middleware.
	handler := middleware.Recovery(panicHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	// This should NOT panic — recovery middleware catches it.
	handler.ServeHTTP(rec, req)

	// Verify HTTP 500.
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	// Verify JSON error response.
	var resp map[string]map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp["error"]["code"] != "internal_error" {
		t.Errorf("error code = %q, want %q", resp["error"]["code"], "internal_error")
	}

	// Verify the panic message is NOT in the response.
	body := rec.Body.String()
	if contains(body, "something went terribly wrong") {
		t.Error("response body should NOT contain the panic message")
	}
}

func TestRecovery_NoPanic_PassesThrough(t *testing.T) {
	normalHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	handler := middleware.Recovery(normalHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestRecovery_NilMapPanic(t *testing.T) {
	// Simulates a common real-world panic: nil map assignment.
	nilMapHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]string
		m["key"] = "value" // PANIC: assignment to entry in nil map
	})

	handler := middleware.Recovery(nilMapHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
