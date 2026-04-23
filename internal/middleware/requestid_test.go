package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
)

func TestRequestID_GeneratesUniqueIDsPerRequest(t *testing.T) {
	var seen []string
	handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, middleware.GetRequestID(r.Context()))
	}))

	for range 3 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.NotEmpty(t, rec.Header().Get("X-Request-ID"), "header must be set")
	}

	require.Len(t, seen, 3)
	require.NotEqual(t, seen[0], seen[1])
	require.NotEqual(t, seen[1], seen[2])
}

func TestGetRequestID_NoID_ReturnsEmpty(t *testing.T) {
	id := middleware.GetRequestID(context.Background())
	require.Empty(t, id)
}

func TestRequestID_HeaderMatchesContext(t *testing.T) {
	var ctxID string
	handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID = middleware.GetRequestID(r.Context())
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	headerID := rec.Header().Get("X-Request-ID")
	require.NotEmpty(t, ctxID)
	require.Equal(t, ctxID, headerID)
}
