package testenv

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
)

// =============================================================================
// Bootstrap — turn the Postgres seed data into chain state.
//
// The flow, called once per testenv.Setup:
//
//   1. Read the DISTINCT set of Ethereum wallet addresses from the DB.
//   2. For each address, set a deterministic native ETH balance.
//   3. For each address, transfer every token defined in fixtures.json
//      from its whale account, using ethutil.TransferERC20 (impersonate +
//      fund-for-gas + tx + receipt wait).
//
// Why this matters didactically: the tests downstream assert that the
// production code (`walletRepo.FindByBlockchain` → `BalanceProvider`)
// observes exactly what the bootstrap planted. It's a closed loop between
// the two layers of state (Postgres + chain) that production usually
// diverges — here we make that divergence visible.
// =============================================================================

// BootstrapChainFromDB drives the whole flow. Exposed (not private) so a
// test can re-run it mid-suite if it has mutated state and wants to
// restore a known baseline.
func (e *Env) BootstrapChainFromDB(ctx context.Context) error {
	addrs, err := e.GetTrackedETHAddresses(ctx)
	if err != nil {
		return fmt.Errorf("read tracked eth wallets: %w", err)
	}
	if len(addrs) == 0 {
		log.Println("testenv.Bootstrap: no ethereum wallets in DB, skipping chain seeding")
		return nil
	}

	rpc := e.ETHClient()
	// native_balance_eth is a float64 for ergonomics; convert to wei just once.
	nativeWei := ethutil.EthToWei(e.Fixtures.NativeBalanceETH)

	log.Printf("testenv.Bootstrap: seeding %d distinct ethereum addresses "+
		"(native=%.0f ETH, tokens=%d)",
		len(addrs), e.Fixtures.NativeBalanceETH, len(e.Fixtures.Tokens))

	for _, addr := range addrs {
		// 1. Native ETH balance.
		if err := ethutil.SetEthBalance(ctx, rpc, addr, ethutil.HexWei(nativeWei)); err != nil {
			return fmt.Errorf("set native balance for %s: %w", addr, err)
		}

		// 2. Every token from fixtures, one after another. Serial on purpose:
		// Anvil applies state atomically per tx and sharing a whale across
		// parallel transfers invites nonce races.
		for sym, tok := range e.Fixtures.Tokens {
			amount, ok := new(big.Int).SetString(tok.Amount, 10)
			if !ok {
				return fmt.Errorf("fixture %s: invalid amount %q", sym, tok.Amount)
			}
			if _, err := ethutil.TransferERC20(ctx, rpc,
				tok.Contract, tok.Whale, addr, amount,
			); err != nil {
				return fmt.Errorf("seed %s → %s: %w", sym, addr, err)
			}
			log.Printf("testenv.Bootstrap: %s %s → %s", sym, humanAmount(tok), short(addr))
		}
	}

	log.Printf("testenv.Bootstrap: done")
	return nil
}

// humanAmount renders the raw-units amount in "tokens" for logs
// (e.g. 1000000000 / 10^6 → 1000). Best-effort; precision is lossy but the
// log line is for humans.
func humanAmount(tok TokenFixture) string {
	raw, ok := new(big.Int).SetString(tok.Amount, 10)
	if !ok {
		return tok.Amount
	}
	f := new(big.Float).SetInt(raw)
	f.Quo(f, big.NewFloat(math.Pow10(tok.Decimals)))
	v, _ := f.Float64()
	return fmt.Sprintf("%.2f", v)
}

// short truncates a hex address for log readability.
func short(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:8] + "…"
}
