package ethutil_test

import (
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
)

// TestERC20TransferCalldata_HappyPath checks the on-the-wire layout of the
// transfer calldata against a known-good fixture.
//
//	0xa9059cbb                                                           // selector
//	000000000000000000000000c0ffee254729296a45a3885639ac7e10f9d54979     // recipient, padded
//	00000000000000000000000000000000000000000000000000000000000f4240     // 1_000_000
func TestERC20TransferCalldata_HappyPath(t *testing.T) {
	got, err := ethutil.ERC20TransferCalldata(
		"0xc0ffee254729296a45a3885639AC7E10F9d54979",
		big.NewInt(1_000_000),
	)
	require.NoError(t, err)
	want := "0xa9059cbb" +
		"000000000000000000000000c0ffee254729296a45a3885639ac7e10f9d54979" +
		"00000000000000000000000000000000000000000000000000000000000f4240"
	require.Equal(t, want, got)
}

func TestERC20TransferCalldata_LargeAmount(t *testing.T) {
	// 2^64 — forces more than 16 hex chars in the amount field.
	amount := new(big.Int).Lsh(big.NewInt(1), 64)
	got, err := ethutil.ERC20TransferCalldata(
		"0x0000000000000000000000000000000000000001",
		amount,
	)
	require.NoError(t, err)
	// 0x aside, total should be 4 + 32 + 32 = 68 hex bytes = 136 chars + "0x" = 138.
	require.Len(t, got, 138)
	// Selector preserved.
	require.Equal(t, "0xa9059cbb", got[:10])
}

func TestERC20TransferCalldata_RejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name      string
		recipient string
		amount    *big.Int
		contains  string
	}{
		{
			name:      "recipient too short",
			recipient: "0xabc",
			amount:    big.NewInt(1),
			contains:  "recipient",
		},
		{
			name:      "recipient too long",
			recipient: "0x" + strings.Repeat("ab", 24), // 48 hex chars
			amount:    big.NewInt(1),
			contains:  "recipient",
		},
		{
			name:      "nil amount",
			recipient: "0x0000000000000000000000000000000000000001",
			amount:    nil,
			contains:  "amount is nil",
		},
		{
			name:      "negative amount",
			recipient: "0x0000000000000000000000000000000000000001",
			amount:    big.NewInt(-1),
			contains:  "negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ethutil.ERC20TransferCalldata(tt.recipient, tt.amount)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.contains)
		})
	}
}

func TestSendTransaction_NilClient(t *testing.T) {
	_, err := ethutil.SendTransaction(t.Context(), nil, "0x1", "0x2", "0x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc client is nil")
}

func TestWaitForReceipt_NilClient(t *testing.T) {
	_, err := ethutil.WaitForReceipt(t.Context(), nil, "0xabc", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc client is nil")
}

func TestTransferERC20_NilClient(t *testing.T) {
	_, err := ethutil.TransferERC20(t.Context(), nil,
		"0x1", "0x2", "0x3", big.NewInt(1))
	require.Error(t, err)
}
