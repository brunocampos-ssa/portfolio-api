package testenv

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

// =============================================================================
// Fixtures define the on-chain state that Bootstrap applies for every tracked
// Ethereum wallet pulled from the database.
//
// The JSON lives next to this source file and is embedded so the test binary
// is self-contained. If you want to seed more tokens, or point at a different
// whale, edit fixtures.json rather than Go code.
//
// On whale stability: USDC → Polygon ERC-20 Bridge (holds hundreds of
// millions for years); USDT → Binance hot wallet (same story). If either
// whale ever runs dry at the fork block, swap the `whale` field.
// =============================================================================

//go:embed fixtures.json
var fixturesRaw []byte

// Fixtures is the parsed fixtures.json — loaded once during testenv.Setup
// and stored on the *Env so tests can reference the same numbers they were
// bootstrapped with when writing assertions.
type Fixtures struct {
	NativeBalanceETH float64                 `json:"native_balance_eth"`
	Tokens           map[string]TokenFixture `json:"tokens"`
}

// TokenFixture describes a single ERC-20 that Bootstrap will transfer from
// `Whale` into every tracked wallet.
type TokenFixture struct {
	Contract string `json:"contract"`
	Whale    string `json:"whale"`
	// Amount is a decimal string of the raw uint256 (i.e. `1000 * 10^decimals`
	// for 1000 USDC). Kept as string to avoid float-rounding surprises for
	// large 18-decimal tokens.
	Amount   string `json:"amount"`
	Decimals int    `json:"decimals"`
}

// loadFixtures parses the embedded fixtures.json. It is called once per
// Setup and the result is cached on *Env.
func loadFixtures() (Fixtures, error) {
	var f Fixtures
	if err := json.Unmarshal(fixturesRaw, &f); err != nil {
		return Fixtures{}, fmt.Errorf("testenv: decode fixtures.json: %w", err)
	}
	if f.NativeBalanceETH <= 0 {
		return Fixtures{}, fmt.Errorf("testenv: fixtures.native_balance_eth must be > 0")
	}
	for sym, tok := range f.Tokens {
		if tok.Contract == "" || tok.Whale == "" || tok.Amount == "" {
			return Fixtures{}, fmt.Errorf("testenv: fixtures.tokens[%s] has an empty required field", sym)
		}
	}
	return f, nil
}
