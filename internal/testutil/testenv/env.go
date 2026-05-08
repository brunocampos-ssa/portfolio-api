// Package testenv boots and wires the full integration-test environment
// (Postgres + forked-mainnet Anvil) so integration test files can focus on
// business behaviour rather than infrastructure.
//
// Typical use from a test file's TestMain:
//
//	var env *testenv.Env
//
//	func TestMain(m *testing.M) {
//	    ctx := context.Background()
//
//	    var err error
//	    env, err = testenv.Setup(ctx)
//	    if err != nil {
//	        log.Fatalf("testenv.Setup: %v", err)
//	    }
//
//	    code := m.Run()
//	    _ = env.Close(context.Background())
//	    os.Exit(code)
//	}
//
// Setup runs DB migrations (including the seed data in migration 002) and
// then bootstraps the forked chain so every tracked Ethereum wallet has:
//
//   - A deterministic native ETH balance (fixtures.native_balance_eth).
//   - Deterministic ERC-20 balances for each token in fixtures.tokens,
//     transferred from well-known whales via impersonation.
//
// The DB is the source of truth; the chain is seeded to match.
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/anvil"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/kafkatest"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/postgres"
)

const (
	// defaultForkURL is the public mainnet RPC used when TEST_ETH_FORK_URL is
	// not set. It is rate-limited; enough for the class demo. If you want a
	// reliable CI pipeline, point TEST_ETH_FORK_URL at a dedicated provider.
	defaultForkURL = "https://eth.drpc.org/"

	// defaultForkBlock is intentionally empty: we let Anvil pick "latest"
	// at fork time. Pinning the block number is cleaner for reproducibility,
	// but in practice free public RPCs (drpc.org, 1rpc.io, ...) prune old
	// state — `--fork-block-number 20000000` starts to fail once the node
	// discards state roots for that block. The whales in fixtures.json hold
	// massive balances continuously, so "latest" is always safe. Override
	// with Options.ForkBlock when using a full-archive RPC.
	defaultForkBlock = ""

	// defaultStartupTimeout caps how long we wait for containers to come up
	// AND for the bootstrap phase. Forking + ERC-20 transfers through drpc.org
	// are the long tent poles.
	defaultStartupTimeout = 3 * time.Minute

	// envForkURL is the environment variable consulted by Setup for an
	// override of the fork RPC.
	envForkURL = "TEST_ETH_FORK_URL"
)

// Options customise the environment boot. Zero values fall back to sensible
// defaults; most callers pass no options at all.
type Options struct {
	// ForkURL overrides the JSON-RPC endpoint passed to `anvil --fork-url`.
	// When empty, Setup consults TEST_ETH_FORK_URL and finally falls back to
	// defaultForkURL.
	ForkURL string

	// ForkBlock is the decimal block number passed as `--fork-block-number`.
	// Pin this so whale balances are reproducible across runs.
	ForkBlock string

	// StartupTimeout caps the combined container boot + bootstrap wait.
	StartupTimeout time.Duration

	// SkipBootstrap, when true, leaves the forked chain untouched. Useful for
	// tests that want to verify Bootstrap themselves, or don't need seeded
	// balances.
	SkipBootstrap bool
}

// Env is the live integration-test environment.
type Env struct {
	DB          *sql.DB
	DatabaseURL string
	Anvil       *anvil.Handle
	Postgres    *postgres.Handle
	Kafka       *kafkatest.Handle
	Fixtures    Fixtures
}

// KafkaBrokers returns the bootstrap server list for the test Kafka
// container. Empty slice on a nil Env or Env without Kafka.
func (e *Env) KafkaBrokers() []string {
	if e == nil || e.Kafka == nil {
		return nil
	}
	return e.Kafka.Brokers
}

// RPCURL returns the http://host:port endpoint of the forked Anvil.
func (e *Env) RPCURL() string {
	if e == nil || e.Anvil == nil {
		return ""
	}
	return e.Anvil.RPCURL()
}

// ETHClient returns the JSON-RPC client bound to the forked Anvil.
func (e *Env) ETHClient() *ethutil.Client {
	if e == nil || e.Anvil == nil {
		return nil
	}
	return e.Anvil.Client()
}

// TransferERC20 is a convenience wrapper over ethutil.TransferERC20 using
// the env's RPC client. Returns the tx hash once mined.
func (e *Env) TransferERC20(ctx context.Context, token, whale, to string, amount *big.Int) (string, error) {
	return ethutil.TransferERC20(ctx, e.ETHClient(), token, whale, to, amount)
}

// SeedNativeBalance sets the native ETH balance of `addr` on the forked
// chain. Used by Bootstrap but also exposed so tests that need a custom
// balance (not matching the fixture default) can override it.
func (e *Env) SeedNativeBalance(ctx context.Context, addr string, wei *big.Int) error {
	return ethutil.SetEthBalance(ctx, e.ETHClient(), addr, ethutil.HexWei(wei))
}

// SeedToken looks `tokenSymbol` up in fixtures and transfers the configured
// amount from that token's whale to `addr`. Returns the resulting tx hash.
func (e *Env) SeedToken(ctx context.Context, addr, tokenSymbol string) (string, error) {
	tok, ok := e.Fixtures.Tokens[tokenSymbol]
	if !ok {
		return "", fmt.Errorf("testenv.SeedToken: unknown token %q", tokenSymbol)
	}
	amount, ok := new(big.Int).SetString(tok.Amount, 10)
	if !ok {
		return "", fmt.Errorf("testenv.SeedToken: invalid amount %q for %s", tok.Amount, tokenSymbol)
	}
	return e.TransferERC20(ctx, tok.Contract, tok.Whale, addr, amount)
}

// Setup boots Postgres and a forked mainnet Anvil container in parallel,
// applies DB migrations, loads fixtures, and (unless SkipBootstrap is set)
// seeds the forked chain from the tracked Ethereum wallets in the DB.
//
// On any failure, whatever was successfully started is cleaned up before
// the error is returned — no partial containers are leaked.
func Setup(ctx context.Context, opts ...Options) (*Env, error) {
	// Load the project's .env (if any) BEFORE resolveOptions runs, so
	// values like TEST_ETH_FORK_URL kick in for `go test` invocations
	// that didn't `source .env` first. Pre-existing env vars win — see
	// loadDotenv for the full semantics.
	if err := loadDotenv(); err != nil {
		return nil, fmt.Errorf("testenv.Setup: load .env: %w", err)
	}

	opt := resolveOptions(opts...)

	bootCtx, cancel := context.WithTimeout(ctx, opt.StartupTimeout)
	defer cancel()

	type pgResult struct {
		h   *postgres.Handle
		err error
	}
	type anvilResult struct {
		h   *anvil.Handle
		err error
	}
	type kafkaResult struct {
		h   *kafkatest.Handle
		err error
	}

	pgCh := make(chan pgResult, 1)
	anvilCh := make(chan anvilResult, 1)
	kafkaCh := make(chan kafkaResult, 1)

	go func() {
		h, err := postgres.Start(bootCtx)
		pgCh <- pgResult{h, err}
	}()
	go func() {
		h, err := anvil.Start(bootCtx, anvil.Options{
			AnvilArgs:      buildAnvilForkArgs(opt.ForkURL, opt.ForkBlock),
			StartupTimeout: opt.StartupTimeout,
		})
		anvilCh <- anvilResult{h, err}
	}()
	go func() {
		h, err := kafkatest.Start(bootCtx)
		kafkaCh <- kafkaResult{h, err}
	}()

	pg := <-pgCh
	av := <-anvilCh
	kf := <-kafkaCh

	// cleanup is idempotent and tolerates nil handles — safe to call on
	// any Setup failure path. Stops EVERY container so nothing leaks
	// when one side starts successfully and another errors.
	cleanup := func() {
		if av.h != nil {
			_ = av.h.Stop(context.Background())
		}
		if pg.h != nil {
			_ = pg.h.Stop(context.Background())
		}
		if kf.h != nil {
			_ = kf.h.Stop(context.Background())
		}
	}
	if pg.err != nil {
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: postgres: %w", pg.err)
	}
	if av.err != nil {
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: anvil fork: %w", av.err)
	}
	if kf.err != nil {
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: kafka: %w", kf.err)
	}

	db, err := sql.Open("postgres", pg.h.DSN)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: open postgres: %w", err)
	}
	// bootCtx (not caller ctx) enforces StartupTimeout end-to-end: if anything
	// after the container boot hangs, it trips the same deadline that capped
	// container startup.
	if err := db.PingContext(bootCtx); err != nil {
		_ = db.Close()
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: ping postgres: %w", err)
	}
	// NOTE: golang-migrate's migrate.Up has no context parameter (see
	// migrate/v4). The boot timeout still applies to the surrounding Setup
	// call — if migrations hang, the Go runtime has no way to unblock them
	// here short of killing the process. A pragmatic fix would be to wrap
	// RunMigrations in a goroutine + select, but that is overkill given how
	// small our migration set is.
	if err := postgres.RunMigrations(db); err != nil {
		_ = db.Close()
		cleanup()
		return nil, fmt.Errorf("testenv.Setup: migrations: %w", err)
	}

	fx, err := loadFixtures()
	if err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}

	env := &Env{
		DB:          db,
		DatabaseURL: pg.h.DSN,
		Anvil:       av.h,
		Postgres:    pg.h,
		Kafka:       kf.h,
		Fixtures:    fx,
	}

	if !opt.SkipBootstrap {
		if err := env.BootstrapChainFromDB(bootCtx); err != nil {
			_ = db.Close()
			cleanup()
			return nil, fmt.Errorf("testenv.Setup: bootstrap: %w", err)
		}
	}

	return env, nil
}

// Close tears down the DB handle, the forked Anvil container, AND the
// Postgres container. It is idempotent and safe on a partially-built Env
// returned from a failed Setup. The first error encountered is returned;
// later teardown steps still run.
func (e *Env) Close(ctx context.Context) error {
	var firstErr error
	capture := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.DB != nil {
		capture(e.DB.Close())
	}
	if e.Anvil != nil {
		capture(e.Anvil.Stop(ctx))
	}
	if e.Postgres != nil {
		capture(e.Postgres.Stop(ctx))
	}
	if e.Kafka != nil {
		capture(e.Kafka.Stop(ctx))
	}
	return firstErr
}

func resolveOptions(opts ...Options) Options {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.ForkURL == "" {
		o.ForkURL = os.Getenv(envForkURL)
		if o.ForkURL == "" {
			o.ForkURL = defaultForkURL
		}
	}
	// ForkBlock deliberately left as-is (empty by default). Setting nothing
	// here is semantically different from setting defaultForkBlock — empty
	// means "omit --fork-block-number, let Anvil use latest".
	if o.ForkBlock == "" && defaultForkBlock != "" {
		o.ForkBlock = defaultForkBlock
	}
	if o.StartupTimeout <= 0 {
		o.StartupTimeout = defaultStartupTimeout
	}
	return o
}

// buildAnvilForkArgs assembles the Anvil command-line flags for a forked
// chain. `--fork-block-number` is omitted entirely when forkBlock is empty,
// so Anvil falls back to the RPC's latest block.
func buildAnvilForkArgs(forkURL, forkBlock string) string {
	args := fmt.Sprintf("--fork-url %s", forkURL)
	if forkBlock != "" {
		args += fmt.Sprintf(" --fork-block-number %s", forkBlock)
	}
	return args
}
