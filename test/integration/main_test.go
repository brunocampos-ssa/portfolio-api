//go:build integration

package integration

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/testenv"
)

// env is the shared Postgres + forked-mainnet Anvil environment used by
// every integration test in this package. It is created once in TestMain so
// the container boot and chain-bootstrap cost is amortised across every
// test that runs.
var env *testenv.Env

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error
	env, err = testenv.Setup(ctx)
	if err != nil {
		log.Fatalf("integration: testenv.Setup: %v", err)
	}

	code := m.Run()

	_ = env.Close(context.Background())
	os.Exit(code)
}
