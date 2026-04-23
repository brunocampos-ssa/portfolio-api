// Package anvil spins up the custom Anvil Docker image defined under
// test/infrastructure/anvil via testcontainers-go, exposing helpers for
// integration tests.
//
// Usage:
//
//	ctx := t.Context()
//	h, err := anvil.Start(ctx)
//	require.NoError(t, err)
//	t.Cleanup(func() { _ = h.Stop(context.Background()) })
//
//	// h.RPCURL() is the http://host:port URL reachable from the test.
//	// h.Client() is a ready-to-use ethutil.Client bound to that URL.
//
// The Dockerfile itself is embedded via `go:embed` from the sibling
// package `test/infrastructure/anvil`. That keeps the build context
// hermetic: we do not rely on runtime.Caller trickery to find files on
// disk, and the test binary carries everything it needs.
package anvil

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/ethutil"
	anvilimg "github.com/brunocampos-ssa/portfolio-api/test/infrastructure/anvil"
)

// Options tune the underlying anvil invocation.
// Zero values produce a sensible default (empty-state, no fork, instant mining).
type Options struct {
	// AnvilArgs is forwarded verbatim into the ANVIL_ARGS env var of the
	// container. Use this to enable forking (`--fork-url …`) or tweak
	// block times (`--block-time 1`).
	AnvilArgs string
	// StartupTimeout caps how long we wait for anvil to come online.
	// Defaults to 60s. CI machines sometimes need more; override as needed.
	StartupTimeout time.Duration
}

// Handle is the live Anvil instance returned by Start.
type Handle struct {
	container tc.Container
	rpcURL    string
	client    *ethutil.Client
}

// RPCURL returns the http://host:port URL reachable from the host machine.
func (h *Handle) RPCURL() string { return h.rpcURL }

// Client returns a ready ethutil.Client pointed at this Anvil instance.
// Callers may freely build additional Client instances with NewClient(h.RPCURL(), ...)
// — the return value is just a convenience.
func (h *Handle) Client() *ethutil.Client { return h.client }

// Stop terminates the underlying container. Tests typically call this from
// t.Cleanup. The caller's context is forwarded to testcontainers for teardown
// tracing; pass context.Background() from Cleanup closures.
func (h *Handle) Stop(ctx context.Context) error {
	if h.container == nil {
		return nil
	}
	return h.container.Terminate(ctx)
}

// Start builds and launches the custom Anvil image from the embedded
// Dockerfile. It blocks until Anvil responds on its public port.
func Start(ctx context.Context, opts ...Options) (*Handle, error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.StartupTimeout <= 0 {
		opt.StartupTimeout = 60 * time.Second
	}

	contextArchive, err := buildDockerfileArchive(anvilimg.FS)
	if err != nil {
		return nil, fmt.Errorf("anvil.Start: build context archive: %w", err)
	}

	req := tc.ContainerRequest{
		FromDockerfile: tc.FromDockerfile{
			// ContextArchive is a tar stream containing the build context.
			// Using it instead of Context means we do not depend on the
			// filesystem layout of the repository at test time.
			ContextArchive: contextArchive,
			Dockerfile:     "Dockerfile",
			KeepImage:      true, // avoid rebuilding on every test run
		},
		ExposedPorts: []string{"8545/tcp"},
		Env: map[string]string{
			"ANVIL_ARGS":          opt.AnvilArgs,
			"ANVIL_PORT":          "8545",
			"ANVIL_PORT_INTERNAL": "9000",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8545/tcp"),
			// The "Listening on" line is printed by anvil itself, proving the
			// binary is serving requests (not just the socat socket).
			wait.ForLog("Listening on"),
		).WithDeadline(opt.StartupTimeout),
	}

	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("anvil.Start: create container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("anvil.Start: container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "8545")
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("anvil.Start: mapped port: %w", err)
	}

	url := fmt.Sprintf("http://%s:%s", host, port.Port())
	client := ethutil.NewClient(url, 10*time.Second)

	// Sanity ping: wait up to 10s for eth_blockNumber to succeed. This catches
	// the rare case where the port is listening but anvil is still booting.
	ok := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ethutil.BlockNumber(ctx, client); err == nil {
			ok = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ok {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("anvil.Start: chain never responded to eth_blockNumber on %s", url)
	}

	return &Handle{
		container: container,
		rpcURL:    url,
		client:    client,
	}, nil
}

// buildDockerfileArchive turns the embedded Dockerfile bundle into a tar
// stream suitable for testcontainers-go FromDockerfile.ContextArchive.
//
// Walking the filesystem (rather than hardcoding "Dockerfile") keeps the
// helper forward-compatible: if the bundle grows to include build-time
// scripts or supporting files, they will be included automatically as
// long as the embed directive picks them up.
func buildDockerfileArchive(src fs.FS) (*bytes.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	err := fs.WalkDir(src, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip the embed.go helper and README — the Docker build context
		// only needs the Dockerfile (and any future build-time assets).
		// In practice, embed.FS only exposes files matched by go:embed
		// patterns, so this filter is defensive: if the embed directive
		// ever widens, we still ship a clean build context.
		if path == "embed.go" || path == "README.md" {
			return nil
		}

		data, err := fs.ReadFile(src, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		hdr := &tar.Header{
			Name: path,
			Mode: 0o644,
			Size: int64(len(data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("write header for %s: %w", path, err)
		}
		if _, err := tw.Write(data); err != nil {
			return fmt.Errorf("write body for %s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar writer: %w", err)
	}
	return bytes.NewReader(buf.Bytes()), nil
}
