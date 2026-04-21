package middleware

import (
	"encoding/json"
	"log"
	"net/http"
	"runtime/debug"
)

// Recovery is an HTTP middleware that catches panics and converts them
// into HTTP 500 responses, keeping the server alive.
//
// WHY THIS MATTERS:
// Without recovery middleware, a panic in any handler kills the entire
// HTTP server. In production, this means one bad request can take down
// the service for ALL users.
//
// HOW IT WORKS:
//  1. defer runs a function when the outer function (the handler) returns
//  2. recover() catches the panic value if one occurred
//  3. We log the panic + stack trace for debugging
//  4. We return a safe 500 JSON response to the client
//  5. The server continues serving other requests
//
// IMPORTANT: recover() only works inside a deferred function.
// Calling recover() in normal flow (not during a panic) returns nil.
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				// Log the panic with full stack trace for debugging.
				// In production, send this to an error tracking service.
				log.Printf("PANIC RECOVERED: %v\nStack trace:\n%s",
					rec, debug.Stack())

				// Return a safe JSON response — never expose internal details.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{
						"code":    "internal_error",
						"message": "an unexpected error occurred",
					},
				})
			}
		}()

		next.ServeHTTP(w, r)
	})
}
