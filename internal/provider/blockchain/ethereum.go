package blockchain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// EthereumProvider fetches ETH balances via JSON-RPC.
// Implements contracts.BalanceProvider for the "ethereum" network.
type EthereumProvider struct {
	rpcURL string
	client *http.Client
}

// NewEthereumProvider creates a provider that talks to an Ethereum JSON-RPC endpoint.
//
// Defensive programming: validate that rpcURL is not empty.
// An empty URL would cause cryptic errors later — fail fast instead.
func NewEthereumProvider(rpcURL string) *EthereumProvider {
	if rpcURL == "" {
		panic("blockchain.NewEthereumProvider: rpcURL must not be empty")
	}
	return &EthereumProvider{
		rpcURL: rpcURL,
		client: &http.Client{},
	}
}

// Network returns "ethereum".
func (p *EthereumProvider) Network() string {
	return "ethereum"
}

type ethRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type ethRPCResponse struct {
	Result string `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// GetBalance fetches the ETH balance for a wallet address using eth_getBalance.
//
// Error handling demonstrated here:
//   - Context cancellation/timeout → wraps with domain.ErrUpstreamTimeout
//   - HTTP failures → wraps with domain.ErrProviderUnavailable
//   - RPC errors → wraps with descriptive context using %w
func (p *EthereumProvider) GetBalance(ctx context.Context, walletAddress string) (float64, string, error) {
	const op = "EthereumProvider.GetBalance"

	reqBody := ethRPCRequest{
		JSONRPC: "2.0",
		Method:  "eth_getBalance",
		Params:  []any{walletAddress, "latest"},
		ID:      1,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, "ETH", fmt.Errorf("%s: marshal request: %w", op, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.rpcURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, "ETH", fmt.Errorf("%s: create request: %w", op, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		// Check if the error is due to context deadline exceeded (timeout).
		if ctx.Err() != nil {
			return 0, "ETH", fmt.Errorf("%s: %w: %w", op, domain.ErrUpstreamTimeout, err)
		}
		// Otherwise it's a network/connection error.
		return 0, "ETH", fmt.Errorf("%s: %w: %w", op, domain.ErrProviderUnavailable, err)
	}
	// defer ensures the response body is closed even if we return early
	// due to a JSON parsing error below. Forgetting this leaks connections.
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "ETH", fmt.Errorf("%s: read response: %w", op, err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, "ETH", fmt.Errorf("%s: %w: HTTP %d: %s", op, domain.ErrProviderUnavailable, resp.StatusCode, string(data))
	}

	var rpcResp ethRPCResponse
	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return 0, "ETH", fmt.Errorf("%s: unmarshal response: %w", op, err)
	}

	if rpcResp.Error != nil {
		return 0, "ETH", fmt.Errorf("%s: rpc error: %s", op, rpcResp.Error.Message)
	}

	eth, err := weiToETH(rpcResp.Result)
	if err != nil {
		return 0, "ETH", fmt.Errorf("%s: %w", op, err)
	}

	return eth, "ETH", nil
}

// weiToETH converts a hex-encoded Wei value to ETH as float64.
func weiToETH(hexVal string) (float64, error) {
	hexVal = strings.TrimPrefix(hexVal, "0x")

	wei := new(big.Int)
	_, ok := wei.SetString(hexVal, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex value: %s", hexVal)
	}

	weiFloat, _ := new(big.Float).SetInt(wei).Float64()
	eth := weiFloat / math.Pow10(18)

	return eth, nil
}
