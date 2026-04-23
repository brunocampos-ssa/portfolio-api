//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
)

// TestLogsFetcher_AgainstAnvil_NegotiatesJSONRPC proves the production logs
// fetcher successfully negotiates JSON-RPC against our forked Anvil image.
//
// We query a random untracked address that cannot possibly appear in any
// recent Transfer events — the result should be empty. The point of the
// test is not the empty slice but the fact that we round-tripped through
// eth_getLogs without an error.
//
// Why keep this seemingly trivial test? It is the simplest "the production
// EthereumLogsFetcher actually speaks to an Ethereum node" proof we have.
// Anything that breaks JSON-RPC framing will fail here first.
func TestLogsFetcher_AgainstAnvil_NegotiatesJSONRPC(t *testing.T) {
	fetcher := blockchain.NewEthereumLogsFetcher(env.RPCURL())

	logs, _, err := fetcher.FetchLogs(t.Context(), []string{
		"0x1111111111111111111111111111111111111111",
	}, 0)
	require.NoError(t, err, "eth_getLogs should round-trip against the fork")
	require.Empty(t, logs, "no Transfer events expected for an untracked address")
}

// TestAnvilHelpers_RoundTrip exercises the mutation helpers against the live
// container. Separate from the snapshot/watcher tests so a failure here
// localises the issue to the helpers themselves.
func TestAnvilHelpers_RoundTrip(t *testing.T) {
	rpc := env.ETHClient()
	ctx := t.Context()

	const addr = "0xabCDef1234567890abCdEf1234567890ABCdef12"

	// Set balance and read it back using the production provider.
	require.NoError(t, ethutil.SetEthBalance(ctx, rpc, addr, ethutil.HexWei(ethutil.EthToWei(7))))

	provider := blockchain.NewEthereumProvider(env.RPCURL())
	got, asset, err := provider.GetBalance(ctx, addr)
	require.NoError(t, err)
	require.Equal(t, "ETH", asset)
	require.InDelta(t, 7.0, got, 1e-9)

	// Storage slot round-trip: write then read via eth_getStorageAt.
	const slot = "0x0000000000000000000000000000000000000000000000000000000000000007"
	const value = "0x00000000000000000000000000000000000000000000000000000000deadbeef"
	require.NoError(t, ethutil.SetStorageAt(ctx, rpc, addr, slot, value))
	gotStorage, err := ethutil.GetStorageAt(ctx, rpc, addr, slot)
	require.NoError(t, err)
	require.Equal(t, value, gotStorage)
}
