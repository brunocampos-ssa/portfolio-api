// Package integration holds end-to-end tests for portfolio-api.
//
// These tests spin up real Postgres and Anvil containers via
// internal/testutil/containers and exercise the existing watcher / snapshot
// code paths against them. They are gated behind the `integration` build
// tag to keep `go test ./...` fast by default.
//
// Run them with:
//
//	make test-integration
//	go test -tags=integration ./test/integration/...
//
// Prerequisites:
//   - Docker daemon reachable by testcontainers-go.
//   - The anvil test image builds from test/infrastructure/anvil/Dockerfile —
//     testcontainers handles the build lazily; the first run is slow.
package integration
