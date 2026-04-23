package ethutil

import (
	"context"
	"fmt"
	"math/big"
	"strings"
)

// =============================================================================
// Anvil state-mutation helpers.
//
// Anvil exposes a handful of non-standard JSON-RPC methods that let tests
// shape chain state arbitrarily. The functions below wrap the ones we use:
//
//   anvil_impersonateAccount(addr)           — act as `addr` without a key
//   anvil_setBalance(addr, hexWei)           — set ETH balance of `addr`
//   anvil_setCode(addr, hexBytecode)         — inject bytecode at `addr`
//   anvil_setStorageAt(addr, slotHex, value) — poke a storage slot directly
//   eth_getStorageAt(addr, slotHex, block)   — read a storage slot back
//
// These helpers all take a *Client so tests can inspect one container and
// drive mutations from another goroutine in parallel. All calls propagate
// the caller's context, so timeouts and cancellation work as expected.
// =============================================================================

// Impersonate tells Anvil to accept transactions signed "from" `addr` without
// requiring a private key. Commonly used to move funds out of whale wallets
// captured by a fork.
func Impersonate(ctx context.Context, rpc *Client, addr string) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.Impersonate: rpc client is nil")
	}
	if err := validateAddress(addr); err != nil {
		return fmt.Errorf("ethutil.Impersonate: %w", err)
	}
	if err := rpc.Call(ctx, "anvil_impersonateAccount", []any{addr}, nil); err != nil {
		return fmt.Errorf("ethutil.Impersonate: %w", err)
	}
	return nil
}

// StopImpersonating is the inverse of Impersonate. Rarely needed in tests
// (we usually tear the whole container down between scenarios) but handy
// when reusing a container across sub-tests.
func StopImpersonating(ctx context.Context, rpc *Client, addr string) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.StopImpersonating: rpc client is nil")
	}
	if err := rpc.Call(ctx, "anvil_stopImpersonatingAccount", []any{addr}, nil); err != nil {
		return fmt.Errorf("ethutil.StopImpersonating: %w", err)
	}
	return nil
}

// SetEthBalance overrides the ETH balance of `addr`. The amount is given as a
// hex-encoded wei string (e.g. "0xde0b6b3a7640000" = 1 ETH). Use HexWei to
// convert from a big.Int or float64.
func SetEthBalance(ctx context.Context, rpc *Client, addr string, balanceHex string) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.SetEthBalance: rpc client is nil")
	}
	if err := validateAddress(addr); err != nil {
		return fmt.Errorf("ethutil.SetEthBalance: %w", err)
	}
	if !strings.HasPrefix(balanceHex, "0x") {
		return fmt.Errorf("ethutil.SetEthBalance: balance must be 0x-prefixed hex, got %q", balanceHex)
	}
	if err := rpc.Call(ctx, "anvil_setBalance", []any{addr, balanceHex}, nil); err != nil {
		return fmt.Errorf("ethutil.SetEthBalance: %w", err)
	}
	return nil
}

// SetEOACode plants the given bytecode at `addr`, effectively turning an EOA
// into a contract for tests. `codeHex` must be 0x-prefixed. Pass "0x" to
// clear the code.
func SetEOACode(ctx context.Context, rpc *Client, addr, codeHex string) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.SetEOACode: rpc client is nil")
	}
	if err := validateAddress(addr); err != nil {
		return fmt.Errorf("ethutil.SetEOACode: %w", err)
	}
	if !strings.HasPrefix(codeHex, "0x") {
		return fmt.Errorf("ethutil.SetEOACode: code must be 0x-prefixed hex, got %q", codeHex)
	}
	if err := rpc.Call(ctx, "anvil_setCode", []any{addr, codeHex}, nil); err != nil {
		return fmt.Errorf("ethutil.SetEOACode: %w", err)
	}
	return nil
}

// SetStorageAt writes `value` into storage slot `slot` of `addr`.
// Both `slot` and `value` must be 32-byte hex strings ("0x" + 64 hex chars).
// Use Uint256Hex or AddressHex if you need to build them from typed values.
func SetStorageAt(ctx context.Context, rpc *Client, addr, slot, value string) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.SetStorageAt: rpc client is nil")
	}
	if err := validateAddress(addr); err != nil {
		return fmt.Errorf("ethutil.SetStorageAt: %w", err)
	}
	if err := validateBytes32(slot); err != nil {
		return fmt.Errorf("ethutil.SetStorageAt: slot %w", err)
	}
	if err := validateBytes32(value); err != nil {
		return fmt.Errorf("ethutil.SetStorageAt: value %w", err)
	}
	if err := rpc.Call(ctx, "anvil_setStorageAt", []any{addr, slot, value}, nil); err != nil {
		return fmt.Errorf("ethutil.SetStorageAt: %w", err)
	}
	return nil
}

// GetStorageAt reads storage slot `slot` of `addr` at the latest block.
// Returns the raw 32-byte value as a 0x-prefixed hex string.
func GetStorageAt(ctx context.Context, rpc *Client, addr, slot string) (string, error) {
	if rpc == nil {
		return "", fmt.Errorf("ethutil.GetStorageAt: rpc client is nil")
	}
	if err := validateAddress(addr); err != nil {
		return "", fmt.Errorf("ethutil.GetStorageAt: %w", err)
	}
	if err := validateBytes32(slot); err != nil {
		return "", fmt.Errorf("ethutil.GetStorageAt: slot %w", err)
	}
	var value string
	if err := rpc.Call(ctx, "eth_getStorageAt", []any{addr, slot, "latest"}, &value); err != nil {
		return "", fmt.Errorf("ethutil.GetStorageAt: %w", err)
	}
	return value, nil
}

// Mine asks Anvil to mine `n` blocks immediately. Useful for forcing the
// watcher's `lastBlock` cursor to advance in tests.
func Mine(ctx context.Context, rpc *Client, n int) error {
	if rpc == nil {
		return fmt.Errorf("ethutil.Mine: rpc client is nil")
	}
	if n <= 0 {
		n = 1
	}
	hex := fmt.Sprintf("0x%x", n)
	if err := rpc.Call(ctx, "anvil_mine", []any{hex}, nil); err != nil {
		return fmt.Errorf("ethutil.Mine: %w", err)
	}
	return nil
}

// BlockNumber returns the current head block number.
func BlockNumber(ctx context.Context, rpc *Client) (uint64, error) {
	if rpc == nil {
		return 0, fmt.Errorf("ethutil.BlockNumber: rpc client is nil")
	}
	var hex string
	if err := rpc.Call(ctx, "eth_blockNumber", nil, &hex); err != nil {
		return 0, fmt.Errorf("ethutil.BlockNumber: %w", err)
	}
	return parseHexUint64(hex)
}

// =============================================================================
// Encoding helpers — small enough to live here instead of pulling go-ethereum.
// =============================================================================

// HexWei converts a big.Int wei amount to the 0x-prefixed hex form that
// anvil_setBalance expects. Nil or negative values return "0x0".
func HexWei(wei *big.Int) string {
	if wei == nil || wei.Sign() <= 0 {
		return "0x0"
	}
	return "0x" + wei.Text(16)
}

// EthToWei converts a human-readable ETH amount (as float64) to wei.
// Keep in mind float64 loses precision beyond ~15 significant digits —
// tests that need exact values should build big.Int directly.
func EthToWei(eth float64) *big.Int {
	// 10^18 wei in one ETH.
	ten18 := new(big.Float).SetFloat64(1e18)
	v := new(big.Float).SetFloat64(eth)
	v.Mul(v, ten18)
	wei, _ := v.Int(nil)
	return wei
}

// Uint256Hex converts a big.Int to the 32-byte left-padded hex form used by
// storage slots and ABI-encoded uint256 values.
func Uint256Hex(v *big.Int) string {
	if v == nil {
		return "0x" + strings.Repeat("0", 64)
	}
	hex := v.Text(16)
	if len(hex) > 64 {
		hex = hex[len(hex)-64:]
	}
	return "0x" + strings.Repeat("0", 64-len(hex)) + hex
}

// AddressHex zero-pads a 20-byte address to the 32-byte form used as a
// storage key (for solidity `mapping(address => …)` layouts).
func AddressHex(addr string) string {
	a := strings.TrimPrefix(strings.ToLower(addr), "0x")
	return "0x" + strings.Repeat("0", 64-len(a)) + a
}

// =============================================================================
// Validation
// =============================================================================

func validateAddress(addr string) error {
	if !strings.HasPrefix(addr, "0x") || len(addr) != 42 {
		return fmt.Errorf("invalid address: %q", addr)
	}
	return nil
}

func validateBytes32(hex string) error {
	if !strings.HasPrefix(hex, "0x") {
		return fmt.Errorf("must be 0x-prefixed: %q", hex)
	}
	if len(hex) != 66 { // "0x" + 64 hex chars
		return fmt.Errorf("must be 32 bytes (66 chars incl. 0x), got %d: %q", len(hex), hex)
	}
	return nil
}

func parseHexUint64(hex string) (uint64, error) {
	hex = strings.TrimPrefix(hex, "0x")
	if hex == "" {
		return 0, nil
	}
	v, ok := new(big.Int).SetString(hex, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex uint64: %q", hex)
	}
	return v.Uint64(), nil
}
