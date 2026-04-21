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

// EthereumTokenProvider fetches ERC-20 token balances using eth_call.
// It calls the balanceOf(address) function on ERC-20 contracts.
type EthereumTokenProvider struct {
	rpcURL string
	client *http.Client
}

// NewEthereumTokenProvider creates a provider for fetching ERC-20 balances.
func NewEthereumTokenProvider(rpcURL string) *EthereumTokenProvider {
	if rpcURL == "" {
		panic("blockchain.NewEthereumTokenProvider: rpcURL must not be empty")
	}
	return &EthereumTokenProvider{
		rpcURL: rpcURL,
		client: &http.Client{},
	}
}

// balanceOfSelector is the function selector for balanceOf(address).
// It's the first 4 bytes of keccak256("balanceOf(address)").
const balanceOfSelector = "0x70a08231"

// GetTokenBalance fetches the ERC-20 token balance for a wallet at a specific contract.
// The decimals parameter is needed to convert the raw token amount to human-readable form.
func (p *EthereumTokenProvider) GetTokenBalance(ctx context.Context, walletAddress, contractAddress string, decimals int) (float64, error) {
	const op = "EthereumTokenProvider.GetTokenBalance"

	// Build the balanceOf(address) call data.
	// Format: 4-byte selector + 32-byte zero-padded address
	addr := strings.TrimPrefix(strings.ToLower(walletAddress), "0x")
	callData := balanceOfSelector + fmt.Sprintf("%064s", addr)

	reqBody := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_call",
		"params": []any{
			map[string]string{
				"to":   contractAddress,
				"data": callData,
			},
			"latest",
		},
		"id": 1,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("%s: marshal request: %w", op, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.rpcURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("%s: create request: %w", op, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, fmt.Errorf("%s: %w: %w", op, domain.ErrUpstreamTimeout, err)
		}
		return 0, fmt.Errorf("%s: %w: %w", op, domain.ErrProviderUnavailable, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("%s: read response: %w", op, err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s: %w: HTTP %d", op, domain.ErrProviderUnavailable, resp.StatusCode)
	}

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return 0, fmt.Errorf("%s: unmarshal response: %w", op, err)
	}

	if rpcResp.Error != nil {
		return 0, fmt.Errorf("%s: rpc error: %s", op, rpcResp.Error.Message)
	}

	// Parse the hex result to a float, adjusting for token decimals.
	balance, err := hexToFloat(rpcResp.Result, decimals)
	if err != nil {
		return 0, fmt.Errorf("%s: parse balance: %w", op, err)
	}

	return balance, nil
}

// hexToFloat converts a hex-encoded integer to a float64, dividing by 10^decimals.
func hexToFloat(hexVal string, decimals int) (float64, error) {
	hexVal = strings.TrimPrefix(hexVal, "0x")
	if hexVal == "" || hexVal == "0" {
		return 0, nil
	}

	val := new(big.Int)
	_, ok := val.SetString(hexVal, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex value: %s", hexVal)
	}

	f, _ := new(big.Float).SetInt(val).Float64()
	return f / math.Pow10(decimals), nil
}
