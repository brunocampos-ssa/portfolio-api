// Package anvil bundles the custom Anvil Dockerfile used by integration
// tests. The Dockerfile lives here alongside its README so that the test
// infrastructure stays visible at the repository root, and is exposed to
// Go code via go:embed so tests do not depend on runtime path resolution.
//
// The consumer is internal/testutil/anvil (also `package anvil`), which
// imports this package with an alias and streams the Dockerfile to the
// testcontainers-go build context as a tar archive.
//
// This mirrors the pattern used by the `migrations` package for SQL files.
package anvil

import "embed"

// FS contains the Anvil test-image Dockerfile. Keep the embed pattern
// explicit (one file today, possibly more tomorrow).
//
//go:embed Dockerfile
var FS embed.FS
