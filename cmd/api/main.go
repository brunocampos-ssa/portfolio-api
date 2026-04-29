package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/httpapi"
	"github.com/brunocampos-ssa/portfolio-api/internal/middleware"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/pricing"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

func main() {
	// --- Configuration ---
	// Load and validate configuration at startup.
	// Fail fast if required values are missing.
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
	// Constructors panic if db is nil — defensive programming.
	// This can never happen here (we just validated db), but the
	// constructors protect against misuse in other contexts.
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
	// If no API key, use mock provider — graceful degradation.
	var priceProvider contracts.PriceProvider
	if cfg.CoinGeckoAPIKey != "" {
		priceProvider = pricing.NewCoinGeckoPriceProvider(cfg.CoinGeckoAPIKey)
		log.Println("Price provider: CoinGecko (live)")
	} else {
		priceProvider = pricing.NewMockPriceProvider()
		log.Println("Price provider: Mock (COINGECKO_DEMO_API_KEY not set)")
	}

	// --- Auth primitives (Module 4 Aula 1) ---
	// Hasher: argon2id with OWASP-leaning defaults. Cost is ~50–150 ms
	// per verify on a developer laptop.
	hasher := auth.NewArgon2idHasher()

	// Tokens: HS256, 15-minute access tokens, 30-day refresh tokens.
	// JWT_SIGNING_KEY MUST be set in non-dev environments. In dev we
	// fall back to a deterministic 32-byte value so `make run` works
	// out of the box.
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

	// --- HTTP handlers ---
	apiHandler := httpapi.NewHandler(portfolioService)
	authHandler := httpapi.NewAuthHandler(authService)

	// --- Routing ---
	//
	// Two-mux split for clean auth boundaries:
	//
	//   * publicMux receives endpoints that MUST stay open: /health,
	//     /auth/*, and the educational /debug/panic endpoints.
	//   * protectedMux receives /users/{id}/* — anything that returns
	//     user-owned data. The JWT middleware wraps the entire sub-mux,
	//     so adding a new protected route is just one HandleFunc call
	//     on protectedMux and it inherits authentication automatically.
	//
	// The outer mux dispatches to the right inner mux by path prefix.
	publicMux := http.NewServeMux()
	apiHandler.RegisterPublicRoutes(publicMux)
	authHandler.RegisterRoutes(publicMux)

	protectedMux := http.NewServeMux()
	apiHandler.RegisterProtectedRoutes(protectedMux)

	rootMux := http.NewServeMux()
	// /users/* → JWT-gated subtree
	rootMux.Handle("/users/", middleware.JWT(tokenVerifier)(protectedMux))
	// Everything else → public subtree (the inner mux still does method/path matching)
	rootMux.Handle("/", publicMux)

	// --- Outer middleware stack ---
	// RequestID → Logging → Recovery → rootMux
	// Recovery is innermost so it catches panics from handlers.
	// Logging wraps recovery so it logs the final status code.
	// RequestID is outermost so all layers have access to the ID.
	var h http.Handler = rootMux
	h = middleware.Recovery(h)
	h = middleware.Logging(h)
	h = middleware.RequestID(h)

	addr := fmt.Sprintf(":%s", cfg.Port)

	// http.Server with explicit timeouts. Module 4 also adds these here
	// because once we accept tokens we want bounded request lifetimes —
	// no slowloris.
	server := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("Starting server on %s", addr)
	log.Printf("Endpoints:")
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
	log.Fatal(server.ListenAndServe())
}
