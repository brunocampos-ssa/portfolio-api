package ethutil_test

import (
	"math/big"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
	"github.com/stretchr/testify/require"
)

func TestHexWei(t *testing.T) {
	tests := []struct {
		name string
		in   *big.Int
		want string
	}{
		{"nil", nil, "0x0"},
		{"zero", big.NewInt(0), "0x0"},
		{"negative", big.NewInt(-1), "0x0"},
		{"one wei", big.NewInt(1), "0x1"},
		{"1 eth", ethutil.EthToWei(1), "0xde0b6b3a7640000"},
		{"0.5 eth", ethutil.EthToWei(0.5), "0x6f05b59d3b20000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ethutil.HexWei(tt.in))
		})
	}
}

func TestEthToWei(t *testing.T) {
	// 1 ETH == 10^18 wei
	got := ethutil.EthToWei(1)
	want := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	require.Equal(t, want.String(), got.String())
}

func TestUint256Hex(t *testing.T) {
	tests := []struct {
		name string
		in   *big.Int
		want string
	}{
		{
			"nil",
			nil,
			"0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			"42",
			big.NewInt(42),
			"0x000000000000000000000000000000000000000000000000000000000000002a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ethutil.Uint256Hex(tt.in)
			require.Equal(t, tt.want, got)
			require.Len(t, got, 66)
		})
	}
}

func TestAddressHex(t *testing.T) {
	addr := "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe"
	want := "0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae"
	require.Equal(t, want, ethutil.AddressHex(addr))
}

// TestNilClient_ReturnsFriendlyError verifies the helpers refuse to dereference
// a nil client — a common student mistake.
func TestNilClient_ReturnsFriendlyError(t *testing.T) {
	t.Run("Impersonate", func(t *testing.T) {
		err := ethutil.Impersonate(t.Context(), nil, "0x1111111111111111111111111111111111111111")
		require.Error(t, err)
		require.Contains(t, err.Error(), "rpc client is nil")
	})
	t.Run("SetEthBalance", func(t *testing.T) {
		err := ethutil.SetEthBalance(t.Context(), nil, "0x1111111111111111111111111111111111111111", "0x1")
		require.Error(t, err)
	})
}

func TestSetEthBalance_RejectsNonHex(t *testing.T) {
	// Use a non-nil client pointed at a bogus URL — we won't actually call it
	// because the hex validation trips first.
	rpc := ethutil.NewClient("http://127.0.0.1:1", 0)
	err := ethutil.SetEthBalance(t.Context(), rpc,
		"0x1111111111111111111111111111111111111111", "100")
	require.Error(t, err)
	require.Contains(t, err.Error(), "0x-prefixed hex")
}

func TestValidateAddress_TooShort(t *testing.T) {
	rpc := ethutil.NewClient("http://127.0.0.1:1", 0)
	err := ethutil.Impersonate(t.Context(), rpc, "0xabc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid address")
}

func TestValidateBytes32_WrongLength(t *testing.T) {
	rpc := ethutil.NewClient("http://127.0.0.1:1", 0)
	err := ethutil.SetStorageAt(t.Context(), rpc,
		"0x1111111111111111111111111111111111111111",
		"0x01", // too short
		"0x0000000000000000000000000000000000000000000000000000000000000001",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "slot")
}
