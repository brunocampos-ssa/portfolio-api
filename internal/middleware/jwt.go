package middleware

import (
	"net/http"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
)

// =============================================================================
// JWT middleware
// =============================================================================
//
// JWT wraps a handler so every request must carry a valid bearer token.
// The verified claims land on the request context — handlers downstream
// pull the user out via auth.UserIDFromContext / auth.ClaimsFromContext.
//
// Why is this in middleware/, not httpapi/? Because the same JWT shape is
// also used by the gRPC interceptor (internal/grpcapi/auth_interceptor.go)
// and we want exactly one verifier object configured at startup, threaded
// through both transports. Keeping it here, next to RequestID / Logging /
// Recovery, also matches the project's "middleware lives together" idiom.

// JWT returns a middleware that validates the Authorization header and
// attaches the decoded claims to the request context.
//
// Constructor panics if verifier is nil — programmer error, refuse to start.
func JWT(verifier *auth.TokenVerifier) func(http.Handler) http.Handler {
	if verifier == nil {
		panic("middleware.JWT: verifier must not be nil")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r.Header.Get("Authorization"))
			if !ok {
				writeJSONUnauthorized(w, "missing or malformed Authorization header")
				return
			}

			claims, err := verifier.Verify(token)
			if err != nil {
				writeJSONUnauthorized(w, "invalid or expired token")
				return
			}

			ctx := auth.WithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bearerToken extracts the token portion of an "Authorization: Bearer <token>"
// header. Returns ok=false on any deviation. The check is case-insensitive
// on the scheme name per RFC 7235 §2.1.
func bearerToken(header string) (string, bool) {
	if header == "" {
		return "", false
	}
	const prefix = "bearer "
	if len(header) <= len(prefix) {
		return "", false
	}
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

// writeJSONUnauthorized writes a 401 with the project's standard error envelope.
// We hand-roll this here because importing httpapi.writeJSON would create
// a cycle (httpapi depends on middleware, not the other way around).
func writeJSONUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `Bearer realm="portfolio-api"`)
	w.WriteHeader(http.StatusUnauthorized)
	// Match the JSON envelope produced by httpapi.writeError.
	body := `{"error":{"code":"unauthenticated","message":` + jsonString(message) + `}}`
	_, _ = w.Write([]byte(body))
}

// jsonString produces a minimally-escaped JSON string literal. We only
// need it for fixed messages so we don't pull in encoding/json for the
// 30-byte payloads this file produces.
func jsonString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
