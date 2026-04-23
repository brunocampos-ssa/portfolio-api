// Package integration holds end-to-end tests for portfolio-api.
//
// The environment (Postgres + forked-mainnet Anvil + DB-driven chain
// bootstrap) is orchestrated by internal/testutil/testenv. Each test file
// leans on a shared `env` value populated in TestMain via `testenv.Setup`.
// Tests are gated behind the `integration` build tag so `go test ./...`
// stays fast by default.
//
// Run them with:
//
//	make test-integration
//	go test -tags=integration ./test/integration/...
//
// Prerequisites:
//   - Docker daemon reachable by testcontainers-go.
//   - The Anvil test image builds from test/infrastructure/anvil/Dockerfile
//     (embedded via go:embed); testcontainers caches it after the first run.
package integration
