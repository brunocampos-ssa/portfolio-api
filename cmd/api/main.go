package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/brunocampos-ssa/portfolio-api/gen/portfolio/v1"
	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/grpcapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/httpapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/pricing"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

func main() {
	// --- Configuration ---
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	// --- Database ---
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("FATAL: open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("FATAL: ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// --- Repositories ---
	userRepo := postgres.NewUserRepository(db)
	walletRepo := postgres.NewWalletRepository(db)
	refreshRepo := postgres.NewRefreshTokenRepository(db)

	// --- Blockchain providers ---
	ethProvider := blockchain.NewEthereumProvider(cfg.EthRPCURL)
	klvProvider := blockchain.NewKleverProvider(cfg.KleverBaseURL)
	registry := blockchain.NewProviderRegistry()
	registry.Register(ethProvider)
	registry.Register(klvProvider)
	log.Printf("Registered blockchain providers: ethereum, klever")

	// --- Price provider ---
	var priceProvider contracts.PriceProvider
	if cfg.CoinGeckoAPIKey != "" {
		priceProvider = pricing.NewCoinGeckoPriceProvider(cfg.CoinGeckoAPIKey)
		log.Println("Price provider: CoinGecko (live)")
	} else {
		priceProvider = pricing.NewMockPriceProvider()
		log.Println("Price provider: Mock (COINGECKO_DEMO_API_KEY not set)")
	}

	// --- Auth primitives (shared between HTTP and gRPC) ---
	hasher := auth.NewArgon2idHasher()
	signingKey := []byte(cfg.JWTSigningKey)
	if len(signingKey) < 32 {
		log.Println("WARN: JWT_SIGNING_KEY missing or too short; using insecure dev default — DO NOT SHIP")
		signingKey = []byte("dev-only-key-aaaaaaaaaaaaaaaaaaa")
	}
	tokenIssuer := auth.NewTokenIssuer(signingKey, auth.DefaultAccessTokenTTL)
	tokenVerifier := auth.NewTokenVerifier(signingKey)

	// --- Services ---
	portfolioService := service.NewPortfolioService(userRepo, walletRepo, registry, priceProvider)
	authService := service.NewAuthService(userRepo, refreshRepo, hasher, tokenIssuer, service.DefaultRefreshTokenTTL)

	// --- HTTP server ---
	httpHandler := buildHTTPHandler(portfolioService, authService, tokenVerifier)
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// --- gRPC server ---
	grpcServer := buildGRPCServer(portfolioService, authService, tokenVerifier)
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("FATAL: gRPC listen: %v", err)
	}

	// --- Lifecycle: run both servers concurrently, shut both down on signal ---
	//
	// We use a stop context that fires on SIGINT / SIGTERM. Each server
	// gets its own goroutine; ListenAndServe blocks, so we have to push
	// it onto a goroutine to compose with shutdown.
	//
	// Module 2 introduced errgroup-style patterns. Here we keep it
	// stdlib-only with sync.WaitGroup and channels — same idea, fewer
	// dependencies — so the gRPC chapter doesn't drag in concurrency
	// patterns the lesson hasn't introduced yet.
	stopCtx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSig()

	logRoutes(cfg)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.Printf("HTTP server listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		log.Printf("gRPC server listening on %s", grpcLis.Addr())
		if err := grpcServer.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	<-stopCtx.Done()
	log.Println("Shutdown signal received, draining…")

	// Bounded shutdown — give in-flight requests a few seconds.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	grpcServer.GracefulStop()

	wg.Wait()
	log.Println("Bye.")
}

// buildHTTPHandler wires the REST routing tree (two-mux split + middleware).
func buildHTTPHandler(
	portfolio *service.PortfolioService,
	authSvc *service.AuthService,
	verifier *auth.TokenVerifier,
) http.Handler {
	apiHandler := httpapi.NewHandler(portfolio)
	authHandler := httpapi.NewAuthHandler(authSvc)

	publicMux := http.NewServeMux()
	apiHandler.RegisterPublicRoutes(publicMux)
	authHandler.RegisterRoutes(publicMux)

	protectedMux := http.NewServeMux()
	apiHandler.RegisterProtectedRoutes(protectedMux)

	rootMux := http.NewServeMux()
	rootMux.Handle("/users/", middleware.JWT(verifier)(protectedMux))
	rootMux.Handle("/", publicMux)

	var h http.Handler = rootMux
	h = middleware.Recovery(h)
	h = middleware.Logging(h)
	h = middleware.RequestID(h)
	return h
}

// buildGRPCServer assembles the gRPC server with the auth interceptor and
// every service registered. The standard health service is registered
// last so /grpc.health.v1.Health/Check is always available.
func buildGRPCServer(
	portfolio *service.PortfolioService,
	authSvc *service.AuthService,
	verifier *auth.TokenVerifier,
) *grpc.Server {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcapi.UnaryAuthInterceptor(verifier)),
	)

	pb.RegisterAuthServiceServer(srv, grpcapi.NewAuthServer(authSvc))
	pb.RegisterPortfolioServiceServer(srv, grpcapi.NewPortfolioServer(portfolio))
	pb.RegisterWalletServiceServer(srv, grpcapi.NewWalletServer(portfolio))

	// Standard health checking — clients that probe the server (LB, k8s)
	// expect this. We mark every service Serving once we're up.
	hsrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, hsrv)
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hsrv.SetServingStatus("portfolio.v1.AuthService", healthpb.HealthCheckResponse_SERVING)
	hsrv.SetServingStatus("portfolio.v1.PortfolioService", healthpb.HealthCheckResponse_SERVING)
	hsrv.SetServingStatus("portfolio.v1.WalletService", healthpb.HealthCheckResponse_SERVING)

	// Reflection lets `grpcurl` discover services without a copy of the
	// .proto files. Pure dev convenience; safe to leave on.
	reflection.Register(srv)

	return srv
}

func logRoutes(cfg *config.Config) {
	log.Printf("REST endpoints (port %s):", cfg.Port)
	log.Printf("  POST /auth/register")
	log.Printf("  POST /auth/login")
	log.Printf("  POST /auth/refresh")
	log.Printf("  POST /auth/logout")
	log.Printf("  GET  /health                       (public)")
	log.Printf("  GET  /users/{id}/portfolio         (JWT, self only)")
	log.Printf("  GET  /users/{id}/wallets           (JWT, self only)")
	log.Printf("  POST /users/{id}/wallets           (JWT, self only)")
	log.Printf("  GET  /debug/panic                  (educational only!)")
	log.Printf("  GET  /debug/panic/nilmap           (educational only!)")
	log.Printf("gRPC services (port %s):", cfg.GRPCPort)
	log.Printf("  portfolio.v1.AuthService           (Register, Login)")
	log.Printf("  portfolio.v1.PortfolioService      (GetPortfolio — JWT, self only)")
	log.Printf("  portfolio.v1.WalletService         (ListWallets, AddWallet — JWT, self only)")
	log.Printf("  grpc.health.v1.Health              (Check, Watch)")
}
