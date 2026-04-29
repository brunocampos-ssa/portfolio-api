//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/brunocampos-ssa/portfolio-api/gen/portfolio/v1"
	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/grpcapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/httpapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/pricing"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// =============================================================================
// authStack — runtime fixtures shared between auth_http_test and auth_grpc_test
// =============================================================================
//
// Both transports need the same set of plumbing (auth + portfolio service,
// repos, signing key) wired against the same Postgres. This helper builds
// it once per test, alongside the actual HTTP test server and a gRPC
// server listening on a real port.
//
// We use a real OS port for gRPC (not bufconn) so the integration tests
// exercise the wire — that's what the chapter promises students. bufconn
// is reserved for unit-y service tests where transport choice doesn't
// matter.

const testJWTSigningKey = "test-test-test-test-test-test-tttt"

type authStack struct {
	httpServer *httptest.Server
	grpcServer *grpc.Server
	grpcConn   *grpc.ClientConn
	authClient pb.AuthServiceClient
	portClient pb.PortfolioServiceClient
	walletCli  pb.WalletServiceClient
	authSvc    *service.AuthService
	portSvc    *service.PortfolioService
}

func (s *authStack) close() {
	if s.grpcConn != nil {
		_ = s.grpcConn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

// newAuthStack boots HTTP + gRPC servers wired against the integration
// Postgres. Tests call t.Cleanup(stack.close) so we tear down between tests.
//
// IMPORTANT: this does NOT use a fresh database per test — the testenv
// Postgres is shared across the integration suite (cost-amortised boot).
// Auth tests therefore use unique emails per test (uniqueEmail helper)
// to avoid colliding with other tests in the suite.
func newAuthStack(t *testing.T) *authStack {
	t.Helper()
	require.NotNil(t, env, "testenv.Env must be initialised — check TestMain")

	db := env.DB

	// --- repos
	userRepo := postgres.NewUserRepository(db)
	walletRepo := postgres.NewWalletRepository(db)
	refreshRepo := postgres.NewRefreshTokenRepository(db)

	// --- providers (pricing mocks; blockchain registry empty since auth
	// tests never call wallet endpoints that need on-chain data)
	registry := blockchain.NewProviderRegistry()
	priceProvider := pricing.NewMockPriceProvider()

	// --- auth primitives
	signingKey := []byte(testJWTSigningKey)
	hasher := auth.NewArgon2idHasherWithParams(8, 1, 1) // fast for tests
	issuer := auth.NewTokenIssuer(signingKey, 5*time.Minute)
	verifier := auth.NewTokenVerifier(signingKey)

	// --- services
	portfolioSvc := service.NewPortfolioService(userRepo, walletRepo, registry, priceProvider)
	authSvc := service.NewAuthService(userRepo, refreshRepo, hasher, issuer, time.Hour)

	// --- HTTP test server with the same routing tree main.go builds
	apiHandler := httpapi.NewHandler(portfolioSvc)
	authHandler := httpapi.NewAuthHandler(authSvc)
	publicMux := http.NewServeMux()
	apiHandler.RegisterPublicRoutes(publicMux)
	authHandler.RegisterRoutes(publicMux)
	protectedMux := http.NewServeMux()
	apiHandler.RegisterProtectedRoutes(protectedMux)
	rootMux := http.NewServeMux()
	rootMux.Handle("/users/", middleware.JWT(verifier)(protectedMux))
	rootMux.Handle("/", publicMux)
	httpSrv := httptest.NewServer(middleware.RequestID(middleware.Logging(middleware.Recovery(rootMux))))

	// --- gRPC server on a real ephemeral port
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for gRPC")
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcapi.UnaryAuthInterceptor(verifier)),
	)
	pb.RegisterAuthServiceServer(grpcSrv, grpcapi.NewAuthServer(authSvc))
	pb.RegisterPortfolioServiceServer(grpcSrv, grpcapi.NewPortfolioServer(portfolioSvc))
	pb.RegisterWalletServiceServer(grpcSrv, grpcapi.NewWalletServer(portfolioSvc))
	go func() { _ = grpcSrv.Serve(grpcLis) }()

	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	stack := &authStack{
		httpServer: httpSrv,
		grpcServer: grpcSrv,
		grpcConn:   conn,
		authClient: pb.NewAuthServiceClient(conn),
		portClient: pb.NewPortfolioServiceClient(conn),
		walletCli:  pb.NewWalletServiceClient(conn),
		authSvc:    authSvc,
		portSvc:    portfolioSvc,
	}
	t.Cleanup(stack.close)
	return stack
}

// uniqueEmail produces a unique email per test invocation so concurrent
// or sequential tests in the same suite don't collide on the unique index.
func uniqueEmail(t *testing.T, prefix string) string {
	t.Helper()
	return prefix + "-" + uniqueSuffix() + "@test.local"
}

var suffixCounter int64

func uniqueSuffix() string {
	suffixCounter++
	return time.Now().UTC().Format("150405.000000") + "-" + itoa(int(suffixCounter))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// timeoutCtx returns a context with a sensible timeout for an integration
// HTTP/gRPC call. Auth flow involves an argon2id verify (cheap with the
// fast-test parameters), DB round-trips, and JWT signing — 5 seconds is
// generous.
func timeoutCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}
