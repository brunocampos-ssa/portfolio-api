package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// KleverProvider fetches KLV balances from the Klever blockchain API.
// Implements contracts.BalanceProvider for the "klever" network.
type KleverProvider struct {
	baseURL string
	client  *http.Client
}

// NewKleverProvider creates a provider that talks to the Klever mainnet API.
func NewKleverProvider(baseURL string) *KleverProvider {
	if baseURL == "" {
		panic("blockchain.NewKleverProvider: baseURL must not be empty")
	}
	return &KleverProvider{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// Network returns "klever".
func (p *KleverProvider) Network() string {
	return "klever"
}

// kleverAccountResponse represents the relevant fields from the Klever API.
type kleverAccountResponse struct {
	Data struct {
		Account struct {
			Balance float64 `json:"balance"`
		} `json:"account"`
	} `json:"data"`
	Error string `json:"error"`
	Code  string `json:"code"`
}

// GetBalance fetches the KLV balance for a wallet address.
func (p *KleverProvider) GetBalance(ctx context.Context, walletAddress string) (float64, string, error) {
	const op = "KleverProvider.GetBalance"

	url := fmt.Sprintf("%s/v1.0/address/%s", p.baseURL, walletAddress)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "KLV", fmt.Errorf("%s: create request: %w", op, err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, "KLV", fmt.Errorf("%s: %w: %w", op, domain.ErrUpstreamTimeout, err)
		}
		return 0, "KLV", fmt.Errorf("%s: %w: %w", op, domain.ErrProviderUnavailable, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "KLV", fmt.Errorf("%s: read response: %w", op, err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, "KLV", fmt.Errorf("%s: %w: HTTP %d: %s", op, domain.ErrProviderUnavailable, resp.StatusCode, string(data))
	}

	var accountResp kleverAccountResponse
	if err := json.Unmarshal(data, &accountResp); err != nil {
		return 0, "KLV", fmt.Errorf("%s: unmarshal response: %w", op, err)
	}

	if accountResp.Error != "" {
		return 0, "KLV", fmt.Errorf("%s: %w: %s", op, domain.ErrProviderUnavailable, accountResp.Error)
	}

	// Klever returns balance in precision 6 (micro KLV), convert to KLV.
	balance := accountResp.Data.Account.Balance / 1_000_000

	return balance, "KLV", nil
}
