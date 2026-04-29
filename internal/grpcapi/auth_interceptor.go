package grpcapi

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
)

// =============================================================================
// Authentication interceptor
// =============================================================================
//
// gRPC's equivalent of HTTP middleware is an interceptor. We register a
// unary interceptor that:
//
//   1. Lets a curated set of "public" methods through unauthenticated
//      (Register, Login, and the standard Health service).
//   2. For every other RPC, extracts the bearer token from the
//      "authorization" metadata header, verifies it via the SAME
//      auth.TokenVerifier used by the REST middleware, and stashes the
//      claims onto the context so handlers can read them.
//
// The "same verifier object" point matters pedagogically: students see
// that adding a second transport doesn't multiply the auth surface — the
// crypto core stays in one place.

// publicMethods is the allow-list of RPC FullMethod strings that bypass
// auth. Format is "/<package>.<service>/<rpc>". We hard-code the entries
// rather than parsing the proto so a typo here fails loudly at startup
// (when the test exercises the path) rather than silently exposing a
// new RPC.
var publicMethods = map[string]struct{}{
	"/portfolio.v1.AuthService/Register":           {},
	"/portfolio.v1.AuthService/Login":              {},
	"/grpc.health.v1.Health/Check":                 {},
	"/grpc.health.v1.Health/Watch":                 {},
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      {},
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": {},
}

// UnaryAuthInterceptor returns a grpc.UnaryServerInterceptor that
// enforces JWT authentication for non-public RPCs.
func UnaryAuthInterceptor(verifier *auth.TokenVerifier) grpc.UnaryServerInterceptor {
	if verifier == nil {
		panic("grpcapi.UnaryAuthInterceptor: verifier must not be nil")
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := publicMethods[info.FullMethod]; ok {
			return handler(ctx, req)
		}

		token, ok := bearerFromMetadata(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing or malformed authorization metadata")
		}

		claims, err := verifier.Verify(token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
		}

		return handler(auth.WithClaims(ctx, claims), req)
	}
}

// bearerFromMetadata pulls the token portion of "authorization: Bearer <t>"
// out of the incoming gRPC metadata. The header key MUST be lowercase per
// the gRPC spec — metadata.FromIncomingContext returns keys lowercased.
func bearerFromMetadata(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", false
	}
	header := values[0]
	const prefix = "bearer "
	if len(header) <= len(prefix) {
		return "", false
	}
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(header[len(prefix):])
	if tok == "" {
		return "", false
	}
	return tok, true
}

// requireSelf asserts the JWT subject equals the user_id field on the
// request. It is the gRPC twin of httpapi.requireSelf — same policy,
// different transport.
//
// Returns codes.PermissionDenied on mismatch, codes.Unauthenticated when
// no claims are on context (interceptor wiring bug).
func requireSelf(ctx context.Context, userID string) error {
	if userID == "" {
		return status.Error(codes.InvalidArgument, "user_id is required")
	}
	subject, ok := auth.UserIDFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	if subject != userID {
		return status.Error(codes.PermissionDenied, "you may only access your own resources")
	}
	return nil
}
