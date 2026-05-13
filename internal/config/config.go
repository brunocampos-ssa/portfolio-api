package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration.
// Values come from environment variables with sensible defaults for local dev.
type Config struct {
	// Shared
	Port        string
	GRPCPort    string
	DatabaseURL string

	// API
	EthRPCURL       string
	KleverBaseURL   string
	CoinGeckoAPIKey string

	// Auth (Module 4)
	//
	// JWTSigningKey is the HS256 secret used for access-token signing
	// AND verification. The HTTP middleware and the gRPC interceptor
	// both share this verifier so the same token works on both transports.
	// Must be at least 32 bytes in production. Empty in dev → main.go
	// substitutes an insecure but deterministic default and warns loudly.
	JWTSigningKey string

	// Event Watcher
	EthWSURL            string        // WebSocket endpoint (used if available)
	WatcherPollInterval time.Duration // polling interval for eth_getLogs
	WatcherStartBlock   uint64        // block to start watching from (0 = latest)

	// Snapshot Runner
	SnapshotWorkers int // number of concurrent workers for snapshot generation

	// Class 2 — Messaging.
	//
	// KafkaBrokers is the bootstrap server list for the Kafka cluster.
	// Comma-separated in the env var, slice in code so the publisher
	// can validate each entry independently. Empty in test/dev when
	// the watcher isn't being run against a real broker.
	KafkaBrokers []string

	// RabbitMQURL is the AMQP URL the router/notifier connect to.
	// Standard amqp://user:pass@host:port/ form. Defaults to the
	// docker-compose RabbitMQ on localhost.
	RabbitMQURL string
}

// Load reads configuration from environment variables.
//
// Defensive programming: validate that critical values are present.
// It's better to fail at startup with a clear message than to fail
// at runtime with a cryptic error when the first request hits.
func Load() (*Config, error) {
	cfg := &Config{
		Port:            getEnv("PORT", "8080"),
		GRPCPort:        getEnv("GRPC_PORT", "50051"),
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/portfolio?sslmode=disable"),
		EthRPCURL:       getEnv("ETH_RPC_URL", "https://eth.drpc.org/"),
		KleverBaseURL:   getEnv("KLEVER_API_BASE_URL", "https://api.mainnet.klever.org"),
		CoinGeckoAPIKey: os.Getenv("COINGECKO_DEMO_API_KEY"), // optional

		// Auth — empty allowed in dev; main.go warns loudly and uses a
		// deterministic fallback so `make run` still works.
		JWTSigningKey: os.Getenv("JWT_SIGNING_KEY"),

		// Event watcher defaults
		EthWSURL:          getEnv("ETH_WS_URL", ""),
		WatcherPollInterval: parseDuration("WATCHER_POLL_INTERVAL", 15*time.Second),
		WatcherStartBlock: parseUint64("WATCHER_START_BLOCK", 0),

		// Snapshot runner defaults
		SnapshotWorkers: parseInt("SNAPSHOT_WORKERS", 3),

		// Messaging
		KafkaBrokers: parseCSV("KAFKA_BROKERS", []string{"localhost:9092"}),
		RabbitMQURL:  getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("config: DATABASE_URL is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func parseDuration(key string, fallback time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return d
}

func parseInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return n
}

func parseUint64(key string, fallback uint64) uint64 {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	n, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

// parseCSV reads a comma-separated env var into a string slice. Empty
// or unset env var falls back to the default. Whitespace around each
// entry is trimmed because operators paste configs with arbitrary
// formatting; we should tolerate it.
func parseCSV(key string, fallback []string) []string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parts := strings.Split(val, ",")
	out := parts[:0]
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}
