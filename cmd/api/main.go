package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"

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

	// --- Service ---
	// Constructor panics if any dependency is nil — fail fast.
	portfolioService := service.NewPortfolioService(userRepo, walletRepo, registry, priceProvider)

	// --- HTTP server ---
	handler := httpapi.NewHandler(portfolioService)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Middleware stack — order matters!
	// RequestID → Logging → Recovery → Handler
	//
	// Recovery is innermost so it catches panics from handlers.
	// Logging wraps recovery so it logs the final status code.
	// RequestID is outermost so all layers have access to the ID.
	var h http.Handler = mux
	h = middleware.Recovery(h)
	h = middleware.Logging(h)
	h = middleware.RequestID(h)

	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("Starting server on %s", addr)
	log.Printf("Endpoints:")
	log.Printf("  GET  /health")
	log.Printf("  GET  /users/{id}/portfolio")
	log.Printf("  GET  /users/{id}/wallets")
	log.Printf("  POST /users/{id}/wallets")
	log.Printf("  GET  /debug/panic         (educational only!)")
	log.Printf("  GET  /debug/panic/nilmap  (educational only!)")
	log.Fatal(http.ListenAndServe(addr, h))
}
