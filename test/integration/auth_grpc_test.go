//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/brunocampos-ssa/portfolio-api/gen/portfolio/v1"
)

// =============================================================================
// gRPC end-to-end auth tests
// =============================================================================
//
// Same testenv DB, but the requests go through a real grpc.Server bound
// to an OS port (see authStack). That gives the suite a transport-level
// proof that the wire path works — not just the in-process handlers.
//
// Cross-transport interop: the access token minted by gRPC Login is a
// regular JWT and works on the REST endpoints too. We assert this in
// TestGRPCAuth_TokenAcceptedAcrossTransports — students see that one
// token, two surfaces, no special-casing.

func TestGRPCAuth_HappyPath(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "grpc-happy")
	ctx, cancel := timeoutCtx(t)
	defer cancel()

	// Register (idempotent — accept AlreadyExists if a previous test
	// left the row behind).
	regResp, err := stack.authClient.Register(ctx, &pb.RegisterRequest{
		Name: "GRPC Happy", Email: email, Password: "long-enough-password",
	})
	require.NoError(t, err)
	require.NotEmpty(t, regResp.GetId())

	// Login.
	loginResp, err := stack.authClient.Login(ctx, &pb.LoginRequest{
		Email: email, Password: "long-enough-password",
	})
	require.NoError(t, err)
	require.NotEmpty(t, loginResp.GetAccessToken())
	require.NotEmpty(t, loginResp.GetRefreshToken())

	// GetPortfolio with the access token in metadata.
	authedCtx := withBearerOutgoing(ctx, loginResp.GetAccessToken())
	portfolio, err := stack.portClient.GetPortfolio(authedCtx, &pb.GetPortfolioRequest{
		UserId: regResp.GetId(),
	})
	require.NoError(t, err)
	require.Equal(t, regResp.GetId(), portfolio.GetUserId())
}

func TestGRPCAuth_RejectsMissingMetadata(t *testing.T) {
	stack := newAuthStack(t)
	ctx, cancel := timeoutCtx(t)
	defer cancel()

	_, err := stack.portClient.GetPortfolio(ctx, &pb.GetPortfolioRequest{UserId: "u1"})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	require.Equal(t, codes.Unauthenticated, st.Code())
}

func TestGRPCAuth_RejectsCrossAccount(t *testing.T) {
	stack := newAuthStack(t)
	emailA := uniqueEmail(t, "grpc-alice")
	emailB := uniqueEmail(t, "grpc-bob")
	ctx, cancel := timeoutCtx(t)
	defer cancel()

	regA, err := stack.authClient.Register(ctx, &pb.RegisterRequest{
		Name: "Alice", Email: emailA, Password: "alice-grpc-password",
	})
	require.NoError(t, err)
	regB, err := stack.authClient.Register(ctx, &pb.RegisterRequest{
		Name: "Bob", Email: emailB, Password: "bob-grpc-password",
	})
	require.NoError(t, err)

	loginA, err := stack.authClient.Login(ctx, &pb.LoginRequest{
		Email: emailA, Password: "alice-grpc-password",
	})
	require.NoError(t, err)

	// Alice's token, Bob's user_id → PermissionDenied.
	authedCtx := withBearerOutgoing(ctx, loginA.GetAccessToken())
	_, err = stack.portClient.GetPortfolio(authedCtx, &pb.GetPortfolioRequest{
		UserId: regB.GetId(),
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())

	// Sanity: Alice can read her own.
	_, err = stack.portClient.GetPortfolio(authedCtx, &pb.GetPortfolioRequest{
		UserId: regA.GetId(),
	})
	require.NoError(t, err)
}

func TestGRPCAuth_LoginRejectsWrongPassword(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "grpc-wrongpw")
	ctx, cancel := timeoutCtx(t)
	defer cancel()

	_, err := stack.authClient.Register(ctx, &pb.RegisterRequest{
		Name: "User", Email: email, Password: "right-password-1",
	})
	require.NoError(t, err)

	_, err = stack.authClient.Login(ctx, &pb.LoginRequest{
		Email: email, Password: "WRONG-password",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

// TestGRPCAuth_TokenAcceptedAcrossTransports proves the wire-level
// interop story: a token minted by gRPC Login is accepted by the REST
// middleware on a protected route, with no extra translation. This is
// the headline demo for "one auth core, two transports".
func TestGRPCAuth_TokenAcceptedAcrossTransports(t *testing.T) {
	stack := newAuthStack(t)
	email := uniqueEmail(t, "crosstx")
	ctx, cancel := timeoutCtx(t)
	defer cancel()

	reg, err := stack.authClient.Register(ctx, &pb.RegisterRequest{
		Name: "Cross", Email: email, Password: "long-enough-password",
	})
	require.NoError(t, err)

	loginResp, err := stack.authClient.Login(ctx, &pb.LoginRequest{
		Email: email, Password: "long-enough-password",
	})
	require.NoError(t, err)

	// Take the gRPC-issued access token to the REST API.
	httpGetJSON(t, stack, "/users/"+reg.GetId()+"/portfolio",
		loginResp.GetAccessToken(), 200)
}

// =============================================================================
// helpers
// =============================================================================

func withBearerOutgoing(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
