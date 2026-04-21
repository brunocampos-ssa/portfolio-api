package blockchain

import (
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// ProviderRegistry maps network names to their BalanceProvider implementations.
// The service layer asks the registry for a provider and gets back a
// contracts.BalanceProvider — it never knows the concrete type.
type ProviderRegistry struct {
	providers map[string]contracts.BalanceProvider
}

// NewProviderRegistry creates an empty registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers: make(map[string]contracts.BalanceProvider),
	}
}

// Register adds a provider to the registry, keyed by its network name.
func (r *ProviderRegistry) Register(provider contracts.BalanceProvider) {
	r.providers[provider.Network()] = provider
}

// Get returns the provider for the given network.
//
// If no provider is registered, it returns an error wrapping
// domain.ErrUnsupportedNetwork so callers can use errors.Is to detect this.
func (r *ProviderRegistry) Get(network string) (contracts.BalanceProvider, error) {
	provider, ok := r.providers[network]
	if !ok {
		return nil, fmt.Errorf("registry: %w: %s", domain.ErrUnsupportedNetwork, network)
	}
	return provider, nil
}
