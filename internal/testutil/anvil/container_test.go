package anvil

import (
	"archive/tar"
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	anvilimg "github.com/brunocampos-ssa/portfolio-api/test/infrastructure/anvil"
)

// TestBuildDockerfileArchive_SingleFile walks a minimal fstest.MapFS and
// confirms the resulting tar stream contains the file verbatim. This keeps
// the archive-building logic testable without Docker.
func TestBuildDockerfileArchive_SingleFile(t *testing.T) {
	fakeFS := fstest.MapFS{
		"Dockerfile": &fstest.MapFile{Data: []byte("FROM alpine:3\n")},
	}

	r, err := buildDockerfileArchive(fakeFS)
	require.NoError(t, err)

	entries := readTar(t, r)
	require.Len(t, entries, 1)
	require.Equal(t, "Dockerfile", entries[0].name)
	require.Equal(t, "FROM alpine:3\n", entries[0].body)
}

// TestBuildDockerfileArchive_SkipsMetadata proves we do not ship embed.go
// or README.md into the Docker build context, even if the embed directive
// ever widens to include them.
func TestBuildDockerfileArchive_SkipsMetadata(t *testing.T) {
	fakeFS := fstest.MapFS{
		"Dockerfile": &fstest.MapFile{Data: []byte("FROM alpine:3\n")},
		"embed.go":   &fstest.MapFile{Data: []byte("package anvil")},
		"README.md":  &fstest.MapFile{Data: []byte("# docs")},
	}

	r, err := buildDockerfileArchive(fakeFS)
	require.NoError(t, err)

	entries := readTar(t, r)
	var names []string
	for _, e := range entries {
		names = append(names, e.name)
	}
	require.ElementsMatch(t, []string{"Dockerfile"}, names)
}

// TestStart_EmbeddedContextContainsDockerfile is a smoke-test against the
// real embedded FS (not a fake). It asserts the embedded bundle actually
// contains a Dockerfile — catches accidental file deletions / renames that
// would break integration tests at runtime.
func TestStart_EmbeddedContextContainsDockerfile(t *testing.T) {
	r, err := buildDockerfileArchive(anvilimg.FS)
	require.NoError(t, err)

	entries := readTar(t, r)
	var found bool
	for _, e := range entries {
		if e.name == "Dockerfile" {
			found = true
			require.Contains(t, e.body, "anvil", "Dockerfile should reference anvil")
		}
	}
	require.True(t, found, "embedded bundle must contain a Dockerfile")
}

// --- helpers ---

type tarEntry struct {
	name string
	body string
}

func readTar(t *testing.T, r io.Reader) []tarEntry {
	t.Helper()
	var out []tarEntry
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var buf bytes.Buffer
		_, err = io.Copy(&buf, tr)
		require.NoError(t, err)
		out = append(out, tarEntry{name: strings.TrimPrefix(hdr.Name, "./"), body: buf.String()})
	}
	return out
}
