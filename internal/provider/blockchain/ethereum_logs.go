package blockchain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// EthereumLogsFetcher polls an Ethereum JSON-RPC endpoint for event logs.
// It uses eth_getLogs to fetch ERC-20 Transfer events for tracked addresses.
//
// Design choice: we use HTTP polling instead of WebSocket subscriptions
// because it works with any Ethereum RPC endpoint (no WS required),
// is simpler to implement, and demonstrates the same concurrency patterns.
// A WebSocket implementation could be added as an alternative.
type EthereumLogsFetcher struct {
	rpcURL string
	client *http.Client
}

// NewEthereumLogsFetcher creates a logs fetcher that talks to an Ethereum JSON-RPC endpoint.
func NewEthereumLogsFetcher(rpcURL string) *EthereumLogsFetcher {
	if rpcURL == "" {
		panic("blockchain.NewEthereumLogsFetcher: rpcURL must not be empty")
	}
	return &EthereumLogsFetcher{
		rpcURL: rpcURL,
		client: &http.Client{},
	}
}

// transferEventSignature is the keccak256 hash of "Transfer(address,address,uint256)".
// This is the standard ERC-20 Transfer event signature used as topics[0].
const transferEventSignature = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// FetchLogs retrieves ERC-20 Transfer logs from the Ethereum node.
//
// The filter matches Transfer events where the sender OR recipient
// is one of the tracked addresses. We use topics[1] and topics[2]
// to filter by from/to addresses respectively.
//
// Returns raw JSON log entries and the latest block number seen.
func (f *EthereumLogsFetcher) FetchLogs(ctx context.Context, addresses []string, fromBlock uint64) ([]json.RawMessage, uint64, error) {
	const op = "EthereumLogsFetcher.FetchLogs"

	// Determine the starting block.
	var fromBlockHex string
	if fromBlock == 0 {
		fromBlockHex = "latest"
	} else {
		fromBlockHex = fmt.Sprintf("0x%x", fromBlock)
	}

	// Pad addresses to 32 bytes for topic matching.
	// In ERC-20 Transfer events, topics[1]=from, topics[2]=to are zero-padded.
	paddedAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		paddedAddresses[i] = padAddress(addr)
	}

	// We make two separate calls: one for outgoing (from=tracked) and one for
	// incoming (to=tracked). This is because eth_getLogs topics use AND between
	// positions but OR within a position.

	var allLogs []json.RawMessage
	var maxBlock uint64

	// Fetch outgoing transfers (from one of our tracked addresses).
	outLogs, outBlock, err := f.fetchLogsWithTopics(ctx, fromBlockHex, []any{
		transferEventSignature,
		paddedAddresses, // topics[1] = from address (OR match)
		nil,             // topics[2] = any to address
	})
	if err != nil {
		return nil, 0, fmt.Errorf("%s: fetch outgoing: %w", op, err)
	}
	allLogs = append(allLogs, outLogs...)
	if outBlock > maxBlock {
		maxBlock = outBlock
	}

	// Fetch incoming transfers (to one of our tracked addresses).
	inLogs, inBlock, err := f.fetchLogsWithTopics(ctx, fromBlockHex, []any{
		transferEventSignature,
		nil,              // topics[1] = any from address
		paddedAddresses,  // topics[2] = to address (OR match)
	})
	if err != nil {
		return nil, 0, fmt.Errorf("%s: fetch incoming: %w", op, err)
	}
	allLogs = append(allLogs, inLogs...)
	if inBlock > maxBlock {
		maxBlock = inBlock
	}

	return allLogs, maxBlock, nil
}

func (f *EthereumLogsFetcher) fetchLogsWithTopics(ctx context.Context, fromBlock string, topics []any) ([]json.RawMessage, uint64, error) {
	filter := map[string]any{
		"fromBlock": fromBlock,
		"toBlock":   "latest",
		"topics":    topics,
	}

	reqBody := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"params":  []any{filter},
		"id":      1,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.rpcURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, fmt.Errorf("%w: %w", domain.ErrUpstreamTimeout, err)
		}
		return nil, 0, fmt.Errorf("%w: %w", domain.ErrProviderUnavailable, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("%w: HTTP %d: %s", domain.ErrProviderUnavailable, resp.StatusCode, string(data))
	}

	var rpcResp struct {
		Result []json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return nil, 0, fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, 0, fmt.Errorf("rpc error: %s", rpcResp.Error.Message)
	}

	// Extract the maximum block number from the results.
	var maxBlock uint64
	for _, raw := range rpcResp.Result {
		var logEntry struct {
			BlockNumber string `json:"blockNumber"`
		}
		if err := json.Unmarshal(raw, &logEntry); err == nil {
			if bn, err := parseHexToUint64(logEntry.BlockNumber); err == nil && bn > maxBlock {
				maxBlock = bn
			}
		}
	}

	return rpcResp.Result, maxBlock, nil
}

// padAddress pads an Ethereum address to 32 bytes (64 hex chars) for topic matching.
// Example: "0xabc123..." → "0x000000000000000000000000abc123..."
func padAddress(addr string) string {
	addr = strings.TrimPrefix(strings.ToLower(addr), "0x")
	padded := fmt.Sprintf("%064s", addr)
	return "0x" + padded
}

// parseHexToUint64 parses a hex string to uint64.
func parseHexToUint64(hex string) (uint64, error) {
	hex = strings.TrimPrefix(hex, "0x")
	val := new(big.Int)
	_, ok := val.SetString(hex, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex: %s", hex)
	}
	return val.Uint64(), nil
}
