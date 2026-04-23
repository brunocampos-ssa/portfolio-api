package watcher_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Additional normalizer unit tests — focus on edge cases + error paths.
//
// The existing normalizer_test.go covers the happy paths (incoming, outgoing,
// filter untracked, filter reorg, filter non-Transfer, short topics). The
// cases below fill gaps that often break in production:
//
//   - invalid / non-hex block numbers
//   - zero-value Transfer (should still produce an event)
//   - case-insensitive topic matching and tracked address matching
//   - unknown tokens fall back to "UNKNOWN" without crashing
// =============================================================================

func TestNormalizeTransferLog_InvalidBlockNumber(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xbroken",
		BlockNumber: "0xzzz", // not valid hex
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
		},
		Data: "0x0000000000000000000000000000000000000000000000000000000000000001",
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Error(t, err, "invalid block number should surface as an error")
	require.Nil(t, event)
	require.Contains(t, err.Error(), "parse block number")
}

func TestNormalizeTransferLog_ZeroAmount(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xzero",
		BlockNumber: "0x1",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
		},
		Data: "0x",
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "0.000000", event.Amount)
	require.Equal(t, "outgoing", event.Direction)
}

func TestNormalizeTransferLog_UnknownToken_UsesUnknownSymbol(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xnewtoken",
		BlockNumber: "0x2",
		Address:     "0x1234567890abcdef1234567890abcdef12345678", // not in knownTokens
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
		},
		// 1 unit (decimals not applied because token is unknown).
		Data: "0x0000000000000000000000000000000000000000000000000000000000000001",
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "UNKNOWN", event.TokenSymbol)
	require.Equal(t, "incoming", event.Direction)
}

func TestNormalizeTransferLog_MixedCaseTopicStillMatches(t *testing.T) {
	// The event signature check uses ToLower — prove it with mixed-case input.
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xmixed",
		BlockNumber: "0x3",
		Address:     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
		Topics: []string{
			// Upper-case hex, should still be treated as the Transfer sig.
			strings.ToUpper("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000DE0B295669A9FD93D5F28D9EC85E40F4CB697BAE",
		},
		Data: "0x0000000000000000000000000000000000000000000000000000000000000001",
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "USDC", event.TokenSymbol)
	require.Equal(t, "incoming", event.Direction)
}

// TableDrivenNormalize sanity-checks a bulk set of inputs in one test, which
// makes the intent compact at a glance.
func TestNormalizeTransferLog_TableDriven(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	tests := []struct {
		name      string
		raw       watcher.RawLog
		wantEvent bool
		wantDir   string
	}{
		{
			name: "exactly 3 topics ok",
			raw: watcher.RawLog{
				BlockNumber: "0x1",
				Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
				Topics: []string{
					"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
					"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
					"0x0000000000000000000000001111111111111111111111111111111111111111",
				},
				Data: "0x01",
			},
			wantEvent: true,
			wantDir:   "outgoing",
		},
		{
			name: "four topics ok (future-proof for indexed fields)",
			raw: watcher.RawLog{
				BlockNumber: "0x1",
				Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
				Topics: []string{
					"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
					"0x0000000000000000000000001111111111111111111111111111111111111111",
					"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
					"0xdeadbeef00000000000000000000000000000000000000000000000000000000",
				},
				Data: "0x01",
			},
			wantEvent: true,
			wantDir:   "incoming",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := watcher.NormalizeTransferLog(tt.raw, tracked)
			require.NoError(t, err)
			if tt.wantEvent {
				require.NotNil(t, event)
				require.Equal(t, tt.wantDir, event.Direction)
			} else {
				require.Nil(t, event)
			}
		})
	}
}

// =============================================================================
// Benchmarks — candidates for pprof / `go tool pprof` exploration.
//
// Run with: `go test ./internal/watcher -bench=. -benchmem -run=^$`
// =============================================================================

func BenchmarkNormalizeTransferLog_Hit(b *testing.B) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xabc123",
		BlockNumber: "0xa",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
		},
		Data: "0x0000000000000000000000000000000000000000000000000000000000000001",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = watcher.NormalizeTransferLog(raw, tracked)
	}
}

func BenchmarkNormalizeTransferLog_Miss(b *testing.B) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}
	raw := watcher.RawLog{
		TxHash:      "0xmiss",
		BlockNumber: "0xa",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
			"0x0000000000000000000000003333333333333333333333333333333333333333",
		},
		Data: "0x01",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = watcher.NormalizeTransferLog(raw, tracked)
	}
}
