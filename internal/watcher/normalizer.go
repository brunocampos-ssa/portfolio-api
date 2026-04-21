package watcher

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// transferEventTopic is the keccak256 hash of "Transfer(address,address,uint256)".
// This is the standard ERC-20 Transfer event signature.
const transferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// tokenInfo holds metadata for well-known ERC-20 tokens.
// In a production system, this would come from a database or external registry.
type tokenInfo struct {
	Symbol   string
	Decimals int
}

// knownTokens maps contract addresses (lowercase) to token metadata.
// We include a small fixed set for didactic purposes.
var knownTokens = map[string]tokenInfo{
	"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {Symbol: "USDC", Decimals: 6},
	"0xdac17f958d2ee523a2206206994597c13d831ec7": {Symbol: "USDT", Decimals: 6},
}

// RawLog represents an Ethereum event log as returned by eth_getLogs.
type RawLog struct {
	TxHash      string   `json:"transactionHash"`
	BlockNumber string   `json:"blockNumber"` // hex-encoded
	Address     string   `json:"address"`     // contract address (emitter)
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	Removed     bool     `json:"removed"` // true if reorged out
}

// NormalizeTransferLog converts a raw Ethereum log into a WalletEvent.
// It identifies whether the sender or recipient matches a tracked address.
//
// Parameters:
//   - raw: the Ethereum event log from eth_getLogs
//   - trackedAddresses: map of lowercase address → wallet ID
//
// Returns:
//   - *WalletEvent if the log matches a tracked address
//   - nil if the log is not relevant (not a Transfer, not tracked, reorged)
//   - error only for malformed data that we can't parse
func NormalizeTransferLog(raw RawLog, trackedAddresses map[string]string) (*domain.WalletEvent, error) {
	// Skip removed (reorged) logs — they represent events that were
	// undone by a chain reorganization.
	if raw.Removed {
		return nil, nil
	}

	// Verify this is a Transfer event.
	// ERC-20 Transfer events have exactly 3 indexed topics:
	//   topics[0] = event signature (Transfer(address,address,uint256))
	//   topics[1] = from address (zero-padded to 32 bytes)
	//   topics[2] = to address (zero-padded to 32 bytes)
	if len(raw.Topics) < 3 {
		return nil, nil
	}
	if strings.ToLower(raw.Topics[0]) != transferEventTopic {
		return nil, nil
	}

	// Parse sender and recipient from topics.
	from := topicToAddress(raw.Topics[1])
	to := topicToAddress(raw.Topics[2])

	// Determine direction and which wallet this event belongs to.
	// An event can only belong to one wallet — we check "from" first.
	var walletID, direction string
	if id, ok := trackedAddresses[from]; ok {
		walletID = id
		direction = "outgoing"
	} else if id, ok := trackedAddresses[to]; ok {
		walletID = id
		direction = "incoming"
	} else {
		// Neither sender nor recipient is tracked — skip.
		return nil, nil
	}

	// Parse block number from hex string.
	blockNum, err := parseHexUint64(raw.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("parse block number %q: %w", raw.BlockNumber, err)
	}

	// Parse the transfer amount from the data field.
	// The amount is a uint256 encoded as a 32-byte hex value.
	amount := parseTransferAmount(raw.Data)

	// Resolve token symbol and adjust for decimals.
	contractAddr := strings.ToLower(raw.Address)
	tokenSymbol := "UNKNOWN"
	if info, ok := knownTokens[contractAddr]; ok {
		tokenSymbol = info.Symbol
		if info.Decimals > 0 {
			amount = amount / math.Pow10(info.Decimals)
		}
	}

	// Serialize the raw log for storage — useful for debugging and auditing.
	rawJSON, _ := json.Marshal(raw)

	event := &domain.WalletEvent{
		ID:              fmt.Sprintf("evt%d", time.Now().UnixNano()),
		WalletID:        walletID,
		TxHash:          raw.TxHash,
		BlockNumber:     blockNum,
		ContractAddress: contractAddr,
		EventType:       "transfer",
		Direction:       direction,
		Amount:          fmt.Sprintf("%.6f", amount),
		TokenSymbol:     tokenSymbol,
		RawPayload:      string(rawJSON),
		CreatedAt:       time.Now(),
	}

	return event, nil
}

// topicToAddress extracts an Ethereum address from a 32-byte hex topic.
// ERC-20 event topics zero-pad 20-byte addresses to 32 bytes.
// Example: "0x000000000000000000000000abc123..." → "0xabc123..."
func topicToAddress(topic string) string {
	topic = strings.TrimPrefix(topic, "0x")
	if len(topic) < 40 {
		return ""
	}
	// Take the last 40 hex characters (20 bytes = Ethereum address).
	return strings.ToLower("0x" + topic[len(topic)-40:])
}

// parseHexUint64 converts a hex string (with optional "0x" prefix) to uint64.
func parseHexUint64(hex string) (uint64, error) {
	hex = strings.TrimPrefix(hex, "0x")
	val := new(big.Int)
	_, ok := val.SetString(hex, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex: %s", hex)
	}
	return val.Uint64(), nil
}

// parseTransferAmount extracts the uint256 value from the data field.
// In Transfer events, data contains the non-indexed amount parameter.
func parseTransferAmount(data string) float64 {
	data = strings.TrimPrefix(data, "0x")
	if data == "" {
		return 0
	}

	val := new(big.Int)
	_, ok := val.SetString(data, 16)
	if !ok {
		return 0
	}

	f, _ := new(big.Float).SetInt(val).Float64()
	return f
}
