package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/config"
)

// TestLoad_Defaults verifies that with no env vars set, all defaults kick in.
// The test flips the relevant env vars via t.Setenv so it doesn't pollute
// neighbouring tests.
func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)

	cfg, err := config.Load()
	require.NoError(t, err)

	require.Equal(t, "8080", cfg.Port)
	require.Contains(t, cfg.DatabaseURL, "postgres://")
	require.NotEmpty(t, cfg.EthRPCURL)
	require.Equal(t, 15*time.Second, cfg.WatcherPollInterval)
	require.Equal(t, uint64(0), cfg.WatcherStartBlock)
	require.Equal(t, 3, cfg.SnapshotWorkers)
}

func TestLoad_OverrideFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "9090")
	t.Setenv("ETH_RPC_URL", "http://localhost:8545")
	t.Setenv("WATCHER_POLL_INTERVAL", "500ms")
	t.Setenv("WATCHER_START_BLOCK", "12345")
	t.Setenv("SNAPSHOT_WORKERS", "16")

	cfg, err := config.Load()
	require.NoError(t, err)

	require.Equal(t, "9090", cfg.Port)
	require.Equal(t, "http://localhost:8545", cfg.EthRPCURL)
	require.Equal(t, 500*time.Millisecond, cfg.WatcherPollInterval)
	require.Equal(t, uint64(12345), cfg.WatcherStartBlock)
	require.Equal(t, 16, cfg.SnapshotWorkers)
}

func TestLoad_InvalidDuration_FallsBackToDefault(t *testing.T) {
	clearEnv(t)
	t.Setenv("WATCHER_POLL_INTERVAL", "nope")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, cfg.WatcherPollInterval)
}

func TestLoad_InvalidInt_FallsBackToDefault(t *testing.T) {
	clearEnv(t)
	t.Setenv("SNAPSHOT_WORKERS", "abc")
	t.Setenv("WATCHER_START_BLOCK", "not a number")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, 3, cfg.SnapshotWorkers)
	require.Equal(t, uint64(0), cfg.WatcherStartBlock)
}

func clearEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"PORT", "DATABASE_URL", "ETH_RPC_URL", "KLEVER_API_BASE_URL",
		"COINGECKO_DEMO_API_KEY", "ETH_WS_URL",
		"WATCHER_POLL_INTERVAL", "WATCHER_START_BLOCK",
		"SNAPSHOT_WORKERS",
	}
	for _, k := range keys {
		t.Setenv(k, "")
	}
}
