package testenv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLoadDotenv exercises the .env loader against a temporary project
// root layout: a fake go.mod plus a .env in the same dir. We chdir into
// a nested subdirectory so the walk-up logic is actually under test
// (mirrors how `go test` invokes us from test/integration/).
func TestLoadDotenv(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module x\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte(
		"# leading comment\n"+
			"FROM_DOTENV=hello\n"+
			"\n"+
			"PRESET=ignored-because-already-set\n"+
			"EMPTY=\n"+
			"WITH_SPACES =   value with spaces   \n",
	), 0o644))
	nested := filepath.Join(root, "test", "integration")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	chdir(t, nested)

	t.Setenv("PRESET", "from-shell") // shell wins over .env
	t.Setenv("FROM_DOTENV", "")      // unset (t.Setenv "" still counts as set)
	require.NoError(t, os.Unsetenv("FROM_DOTENV"))
	require.NoError(t, os.Unsetenv("EMPTY"))
	require.NoError(t, os.Unsetenv("WITH_SPACES"))

	require.NoError(t, loadDotenv())

	require.Equal(t, "hello", os.Getenv("FROM_DOTENV"))
	require.Equal(t, "from-shell", os.Getenv("PRESET"), ".env must not override pre-set env")
	_, emptyPresent := os.LookupEnv("EMPTY")
	require.False(t, emptyPresent, "empty values in .env are skipped, not exported as empty")
	require.Equal(t, "value with spaces", os.Getenv("WITH_SPACES"))
}

// TestLoadDotenv_NoEnvFile is a no-op when the project root lacks a
// .env. CI containers and freshly cloned repos rely on this.
func TestLoadDotenv_NoEnvFile(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module x\n"), 0o644))
	chdir(t, root)
	require.NoError(t, loadDotenv())
}

// TestLoadDotenv_MalformedLine surfaces a real bug (missing '='), so we
// fail loudly rather than silently dropping the line.
func TestLoadDotenv_MalformedLine(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module x\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte("BROKEN_LINE_NO_EQUALS\n"), 0o644))
	chdir(t, root)
	err := loadDotenv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "malformed line")
}

// chdir temporarily swaps the working directory and restores it on
// test cleanup. Subtests in the same package would otherwise leak CWD.
func chdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(orig) })
}
