package blockchain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
)

type stubProvider struct {
	network string
}

func (s *stubProvider) Network() string { return s.network }
func (s *stubProvider) GetBalance(_ context.Context, _ string) (float64, string, error) {
	return 0, s.network, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := blockchain.NewProviderRegistry()
	r.Register(&stubProvider{network: "ethereum"})
	r.Register(&stubProvider{network: "klever"})

	eth, err := r.Get("ethereum")
	require.NoError(t, err)
	require.Equal(t, "ethereum", eth.Network())

	klv, err := r.Get("klever")
	require.NoError(t, err)
	require.Equal(t, "klever", klv.Network())
}

func TestRegistry_Get_Unsupported_WrapsSentinel(t *testing.T) {
	r := blockchain.NewProviderRegistry()

	_, err := r.Get("solana")
	require.Error(t, err)
	require.True(t, errors.Is(err, domain.ErrUnsupportedNetwork),
		"registry should wrap ErrUnsupportedNetwork so callers can errors.Is it")
}

// TestRegistry_Register_Overwrites documents the current behaviour: a second
// Register for the same network silently replaces the first provider. If that
// changes in the future the test will flag it.
func TestRegistry_Register_Overwrites(t *testing.T) {
	r := blockchain.NewProviderRegistry()
	r.Register(&stubProvider{network: "ethereum"})
	r.Register(&stubProvider{network: "ethereum"}) // same network, different instance

	got, err := r.Get("ethereum")
	require.NoError(t, err)
	require.Equal(t, "ethereum", got.Network())
}
