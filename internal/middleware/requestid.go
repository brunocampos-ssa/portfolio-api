package middleware

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
)

// requestIDKey is an unexported type to avoid context key collisions.
type requestIDKey struct{}

// RequestID is an HTTP middleware that generates a unique request ID
// and stores it in the request context and response header.
//
// This is useful for correlating logs across different layers:
// handler → service → repository → provider all share the same request ID.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := generateRequestID()

		// Add to response header so the client can reference it in support tickets.
		w.Header().Set("X-Request-ID", id)

		// Store in context so downstream code can access it for logging.
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extracts the request ID from context, if present.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
