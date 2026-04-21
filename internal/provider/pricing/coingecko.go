package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// coinGeckoIDMap maps asset symbols to CoinGecko API IDs.
var coinGeckoIDMap = map[string]string{
	"ETH": "ethereum",
	"KLV": "klever",
	"BTC": "bitcoin",
}

// CoinGeckoPriceProvider fetches live USD prices from the CoinGecko API.
type CoinGeckoPriceProvider struct {
	apiKey string
	client *http.Client
}

// NewCoinGeckoPriceProvider creates a provider with the given demo API key.
func NewCoinGeckoPriceProvider(apiKey string) *CoinGeckoPriceProvider {
	if apiKey == "" {
		panic("pricing.NewCoinGeckoPriceProvider: apiKey must not be empty")
	}
	return &CoinGeckoPriceProvider{
		apiKey: apiKey,
		client: &http.Client{},
	}
}

// GetPriceUSD fetches the current USD price for the given asset symbol.
func (p *CoinGeckoPriceProvider) GetPriceUSD(ctx context.Context, asset string) (float64, error) {
	const op = "CoinGeckoPriceProvider.GetPriceUSD"

	coinID, ok := coinGeckoIDMap[strings.ToUpper(asset)]
	if !ok {
		return 0, fmt.Errorf("%s: %w: %s", op, domain.ErrUnsupportedAsset, asset)
	}

	url := fmt.Sprintf(
		"https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd",
		coinID,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("%s: create request: %w", op, err)
	}
	req.Header.Set("x-cg-demo-api-key", p.apiKey)

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

	var result map[string]map[string]float64
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, fmt.Errorf("%s: unmarshal response: %w", op, err)
	}

	prices, ok := result[coinID]
	if !ok {
		return 0, fmt.Errorf("%s: no price data for %s", op, coinID)
	}

	usd, ok := prices["usd"]
	if !ok {
		return 0, fmt.Errorf("%s: no USD price for %s", op, coinID)
	}

	return usd, nil
}
