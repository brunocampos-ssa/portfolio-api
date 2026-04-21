package pricing

import (
	"context"
	"fmt"
	"strings"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// mockPrices contains hardcoded prices used when no CoinGecko API key is available.
var mockPrices = map[string]float64{
	"ETH": 3500.00,
	"KLV": 0.015,
	"BTC": 95000.00,
}

// MockPriceProvider returns hardcoded prices for testing and development.
// This is a classic example of polymorphism: the service layer doesn't know
// (or care) whether it's talking to CoinGecko or this mock.
type MockPriceProvider struct{}

// NewMockPriceProvider creates a mock price provider.
func NewMockPriceProvider() *MockPriceProvider {
	return &MockPriceProvider{}
}

// GetPriceUSD returns a hardcoded price for the given asset.
func (p *MockPriceProvider) GetPriceUSD(_ context.Context, asset string) (float64, error) {
	const op = "MockPriceProvider.GetPriceUSD"

	price, ok := mockPrices[strings.ToUpper(asset)]
	if !ok {
		return 0, fmt.Errorf("%s: %w: %s", op, domain.ErrUnsupportedAsset, asset)
	}
	return price, nil
}
