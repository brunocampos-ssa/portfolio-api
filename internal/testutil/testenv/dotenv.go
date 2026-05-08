package testenv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// loadDotenv reads KEY=VALUE pairs from the project's .env file (the one
// next to go.mod) and exports them into the process environment. It is
// the only piece of plumbing that lets `go test -tags=integration ./...`
// pick up things like TEST_ETH_FORK_URL without the developer first
// running `source .env` or configuring their shell.
//
// Semantics intentionally match the broader .env ecosystem:
//
//   - A pre-existing process env var is NEVER overwritten. Shell exports,
//     CI secrets, and IDE run configs always win over .env.
//   - An empty value (KEY=) is treated as "not set" — we skip it so the
//     downstream defaulting logic (see resolveOptions) still kicks in.
//   - Lines starting with # and blank lines are comments.
//   - A missing .env file is NOT an error: CI environments and freshly
//     cloned repos may not have one, and that's fine.
//   - Quoted-string handling and ${VAR} interpolation are deliberately
//     out of scope. Keep the surface area small; if a value needs that,
//     export it from the shell.
//
// The walk-up to find go.mod handles the fact that `go test` sets CWD
// to the package directory (test/integration/), not the repo root.
func loadDotenv() error {
	path, err := findDotenvAtProjectRoot()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("testenv: open %s: %w", path, err)
	}
	defer f.Close()

	scan := bufio.NewScanner(f)
	lineNum := 0
	for scan.Scan() {
		lineNum++
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			return fmt.Errorf("testenv: %s:%d: malformed line (expected KEY=VALUE)", path, lineNum)
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if _, alreadySet := os.LookupEnv(key); alreadySet {
			continue
		}
		if val == "" {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			return fmt.Errorf("testenv: setenv %s: %w", key, err)
		}
	}
	return scan.Err()
}

// findDotenvAtProjectRoot walks up from the current working directory
// until it finds a go.mod, then returns the path to .env alongside it.
// Returns os.ErrNotExist if either the project root or the .env file
// at that root is missing — both are non-fatal at the call site.
func findDotenvAtProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			envPath := filepath.Join(dir, ".env")
			if _, err := os.Stat(envPath); err != nil {
				return "", err
			}
			return envPath, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
