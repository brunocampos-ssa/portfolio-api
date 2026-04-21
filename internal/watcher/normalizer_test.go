package watcher_test

import (
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNormalizeTransferLog_IncomingTransfer verifies that a Transfer event
// where the recipient is a tracked address produces an "incoming" event.
func TestNormalizeTransferLog_IncomingTransfer(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0xabc123",
		BlockNumber: "0xa",
		Address:     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111", // from
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae", // to (tracked)
		},
		Data:    "0x0000000000000000000000000000000000000000000000000000000005f5e100", // 100 USDC (100 * 10^6)
		Removed: false,
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	assert.Nil(t, err)
	assert.NotNil(t, event)

	require.Equal(t, "w1", event.WalletID)
	require.Equal(t, "incoming", event.Direction)
	require.Equal(t, "USDC", event.TokenSymbol)
	require.Equal(t, "transfer", event.EventType)
	require.Equal(t, uint64(10), event.BlockNumber)
	require.Equal(t, "0xabc123", event.TxHash)

}

// TestNormalizeTransferLog_OutgoingTransfer verifies that a Transfer event
// where the sender is a tracked address produces an "outgoing" event.
func TestNormalizeTransferLog_OutgoingTransfer(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0xdef456",
		BlockNumber: "0x14",
		Address:     "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae", // from (tracked)
			"0x0000000000000000000000002222222222222222222222222222222222222222", // to
		},
		Data:    "0x0000000000000000000000000000000000000000000000000000000005f5e100",
		Removed: false,
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Nil(t, err)
	require.NotNil(t, event)

	require.Equal(t, "w1", event.WalletID)
	require.Equal(t, "outgoing", event.Direction)
	require.Equal(t, "USDT", event.TokenSymbol)
}

// TestNormalizeTransferLog_UntrackedAddress verifies that events not involving
// any tracked address return nil (filtered out).
func TestNormalizeTransferLog_UntrackedAddress(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0x999",
		BlockNumber: "0x1",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
		},
		Data:    "0x01",
		Removed: false,
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Nil(t, err)
	require.Nil(t, event, "expected nil event for untracked addresses")
}

// TestNormalizeTransferLog_RemovedLog verifies that reorged logs are skipped.
func TestNormalizeTransferLog_RemovedLog(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0xreorged",
		BlockNumber: "0x1",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
		},
		Data:    "0x01",
		Removed: true, // This log was removed due to chain reorganization.
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Nil(t, err)
	require.Nil(t, event, "expected nil event for removed logs")
}

// TestNormalizeTransferLog_NotTransferEvent verifies that non-Transfer
// events are ignored.
func TestNormalizeTransferLog_NotTransferEvent(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0xother",
		BlockNumber: "0x1",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xabcdef1234567890000000000000000000000000000000000000000000000000", // not Transfer
			"0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
		},
		Data:    "0x01",
		Removed: false,
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Nil(t, err)
	require.Nil(t, event, "expected nil event for non-Transfer event")
}

// TestNormalizeTransferLog_InsufficientTopics verifies that logs with
// fewer than 3 topics are ignored gracefully.
func TestNormalizeTransferLog_InsufficientTopics(t *testing.T) {
	tracked := map[string]string{
		"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae": "w1",
	}

	raw := watcher.RawLog{
		TxHash:      "0xshort",
		BlockNumber: "0x1",
		Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
		},
		Data:    "0x01",
		Removed: false,
	}

	event, err := watcher.NormalizeTransferLog(raw, tracked)
	require.Nil(t, err)
	require.Nil(t, event, "expected nil event for insufficient topics")
}
