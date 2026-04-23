package ethutil

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// =============================================================================
// High-level ERC-20 helpers built on top of the minimal JSON-RPC client.
//
// These are what integration tests use to shape token state on a forked
// Anvil chain. Kept here so test files stay focused on business behaviour
// and don't reinvent ABI encoding.
// =============================================================================

// ERC20TransferCalldata builds the ABI-encoded calldata for
//
//	function transfer(address to, uint256 amount)
//
// Wire format:
//
//	0xa9059cbb                             // selector (first 4 bytes of
//	                                       //  keccak256("transfer(address,uint256)"))
//	00…00<20-byte recipient address>       // padded to 32 bytes
//	00…00<uint256 amount>                  // padded to 32 bytes
//
// The selector is hardcoded because recomputing keccak256 would drag in
// an extra dependency. If Solidity's signature ever changes this must be
// updated — unlikely on the order of millennia.
//
// Returns an error (rather than a silent fallback) when inputs are
// malformed, so callers surface the bug instead of sending a transfer to
// the zero address.
func ERC20TransferCalldata(recipient string, amount *big.Int) (string, error) {
	const selector = "0xa9059cbb"
	if err := validateAddress(recipient); err != nil {
		return "", fmt.Errorf("ethutil.ERC20TransferCalldata: recipient: %w", err)
	}
	if amount == nil {
		return "", fmt.Errorf("ethutil.ERC20TransferCalldata: amount is nil")
	}
	if amount.Sign() < 0 {
		return "", fmt.Errorf("ethutil.ERC20TransferCalldata: amount is negative")
	}
	addr := strings.ToLower(strings.TrimPrefix(recipient, "0x"))
	addrPadded := strings.Repeat("0", 64-len(addr)) + addr
	amountHex := amount.Text(16)
	if len(amountHex) > 64 {
		return "", fmt.Errorf("ethutil.ERC20TransferCalldata: amount exceeds uint256")
	}
	amountPadded := strings.Repeat("0", 64-len(amountHex)) + amountHex
	return selector + addrPadded + amountPadded, nil
}

// Receipt is the subset of eth_getTransactionReceipt we care about.
type Receipt struct {
	Status      string `json:"status"`
	BlockNumber string `json:"blockNumber"`
	TxHash      string `json:"transactionHash"`
}

// SendTransaction wraps eth_sendTransaction. With Anvil impersonation active
// for `from`, the node accepts the call without a signed payload — that's the
// whole point of `anvil_impersonateAccount`.
//
// A gas limit of 500_000 is always set so the node doesn't refuse the call
// because it can't estimate gas (happens on some fork setups).
func SendTransaction(ctx context.Context, rpc *Client, from, to, data string) (string, error) {
	if rpc == nil {
		return "", fmt.Errorf("ethutil.SendTransaction: rpc client is nil")
	}
	params := map[string]any{
		"from": from,
		"to":   to,
		"data": data,
		"gas":  "0x7a120", // 500_000
	}
	var hash string
	if err := rpc.Call(ctx, "eth_sendTransaction", []any{params}, &hash); err != nil {
		return "", fmt.Errorf("ethutil.SendTransaction: %w", err)
	}
	return hash, nil
}

// WaitForReceipt polls eth_getTransactionReceipt until the receipt is
// non-null with status=0x1 (success). Errors on status=0x0 (revert) or when
// the deadline is exceeded.
func WaitForReceipt(ctx context.Context, rpc *Client, txHash string, timeout time.Duration) (*Receipt, error) {
	if rpc == nil {
		return nil, fmt.Errorf("ethutil.WaitForReceipt: rpc client is nil")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var r *Receipt
		if err := rpc.Call(ctx, "eth_getTransactionReceipt", []any{txHash}, &r); err != nil {
			return nil, fmt.Errorf("ethutil.WaitForReceipt: %w", err)
		}
		if r != nil {
			switch r.Status {
			case "0x1":
				return r, nil
			case "0x0":
				return r, fmt.Errorf("ethutil.WaitForReceipt: tx %s reverted", txHash)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("ethutil.WaitForReceipt: timeout for tx %s", txHash)
}

// TransferERC20 is the high-level helper used by tests to move tokens out of
// a whale account on a forked chain. It:
//
//  1. Impersonates `whale` via anvil_impersonateAccount.
//  2. Funds `whale` with 10 ETH so gas accounting succeeds.
//  3. Calls `token.transfer(to, amount)`.
//  4. Waits up to 30s for the receipt; returns on status=0x1.
//
// Returns the transaction hash on success.
func TransferERC20(ctx context.Context, rpc *Client, token, whale, to string, amount *big.Int) (string, error) {
	if rpc == nil {
		return "", fmt.Errorf("ethutil.TransferERC20: rpc client is nil")
	}
	if err := Impersonate(ctx, rpc, whale); err != nil {
		return "", err
	}
	if err := SetEthBalance(ctx, rpc, whale, HexWei(EthToWei(10))); err != nil {
		return "", err
	}
	data, err := ERC20TransferCalldata(to, amount)
	if err != nil {
		return "", err
	}
	hash, err := SendTransaction(ctx, rpc, whale, token, data)
	if err != nil {
		return "", err
	}
	if _, err := WaitForReceipt(ctx, rpc, hash, 30*time.Second); err != nil {
		return "", err
	}
	return hash, nil
}
