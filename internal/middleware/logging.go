package middleware

import (
	"log"
	"net/http"
	"time"
)

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.statusCode = code
	sw.ResponseWriter.WriteHeader(code)
}

// Logging is an HTTP middleware that logs each request with method, path,
// status code, and duration.
//
// This demonstrates defer for timing: we capture the start time when
// the request begins, and the deferred function logs the elapsed time
// when the handler returns — regardless of whether it returned normally
// or via an error path.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// The handler runs here. When it returns (even via panic/recover
		// from an inner middleware), the deferred log runs.
		next.ServeHTTP(sw, r)

		// defer is not needed here because we're after ServeHTTP.
		// But in real production code you'd use defer if there were
		// multiple return paths above.
		log.Printf("%s %s → %d (%s)",
			r.Method, r.URL.Path, sw.statusCode, time.Since(start))
	})
}
