package auth_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/auth"
)

// Use a deliberately weak parameter set in tests so the suite stays fast.
// Production code uses the OWASP-leaning defaults baked into NewArgon2idHasher.
func newTestHasher(t *testing.T) *auth.Argon2idHasher {
	t.Helper()
	return auth.NewArgon2idHasherWithParams(8, 1, 1)
}

func TestArgon2idHasher_HashAndVerify_RoundTrip(t *testing.T) {
	h := newTestHasher(t)

	encoded, err := h.Hash("correct horse battery staple")
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	ok, err := h.Verify(encoded, "correct horse battery staple")
	require.NoError(t, err)
	require.True(t, ok, "verify should accept the original password")
}

func TestArgon2idHasher_Verify_RejectsWrongPassword(t *testing.T) {
	h := newTestHasher(t)

	encoded, err := h.Hash("hunter2")
	require.NoError(t, err)

	ok, err := h.Verify(encoded, "hunter3")
	require.NoError(t, err)
	require.False(t, ok, "verify should reject a wrong password")
}

func TestArgon2idHasher_Hash_DistinctSaltsForSamePassword(t *testing.T) {
	h := newTestHasher(t)

	first, err := h.Hash("same-password")
	require.NoError(t, err)

	second, err := h.Hash("same-password")
	require.NoError(t, err)

	require.NotEqual(t, first, second,
		"hashing the same password twice must produce different outputs (random salt)")
}

func TestArgon2idHasher_Hash_RejectsEmptyPassword(t *testing.T) {
	h := newTestHasher(t)

	_, err := h.Hash("")
	require.Error(t, err)
}

func TestArgon2idHasher_Hash_PHCStringShape(t *testing.T) {
	h := newTestHasher(t)

	encoded, err := h.Hash("anything")
	require.NoError(t, err)

	parts := strings.Split(encoded, "$")
	require.Len(t, parts, 6, "PHC string must have exactly 6 fields")
	require.Equal(t, "", parts[0])
	require.Equal(t, "argon2id", parts[1])
	require.Equal(t, "v=19", parts[2])
	require.Contains(t, parts[3], "m=")
	require.Contains(t, parts[3], "t=")
	require.Contains(t, parts[3], "p=")
}

func TestArgon2idHasher_Verify_RejectsTamperedHash(t *testing.T) {
	h := newTestHasher(t)

	encoded, err := h.Hash("hunter2")
	require.NoError(t, err)

	// Flip the last character of the hash field.
	parts := strings.Split(encoded, "$")
	last := parts[5]
	if last[len(last)-1] == 'A' {
		parts[5] = last[:len(last)-1] + "B"
	} else {
		parts[5] = last[:len(last)-1] + "A"
	}
	tampered := strings.Join(parts, "$")

	ok, err := h.Verify(tampered, "hunter2")
	require.NoError(t, err)
	require.False(t, ok, "verify must not accept a tampered hash")
}

func TestArgon2idHasher_Verify_RejectsMalformedHash(t *testing.T) {
	h := newTestHasher(t)

	cases := []struct {
		name string
		hash string
	}{
		{"empty", ""},
		{"random_string", "not-a-phc-string"},
		{"missing_fields", "$argon2id$v=19$m=8,t=1,p=1$salt"},
		{"unsupported_algorithm", "$argon2i$v=19$m=8,t=1,p=1$AAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{"bad_version", "$argon2id$v=99$m=8,t=1,p=1$AAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{"bad_base64", "$argon2id$v=19$m=8,t=1,p=1$!!!notbase64!!!$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := h.Verify(tc.hash, "hunter2")
			if tc.name == "empty" {
				require.NoError(t, err)
				require.False(t, ok)
				return
			}
			// Other malformed cases must report a decode error, not silently
			// return false — the caller should know the column was corrupt.
			require.Error(t, err, "malformed hash should produce a decode error")
			require.False(t, ok)
		})
	}
}

func TestNewArgon2idHasherWithParams_RejectsBrokenInputs(t *testing.T) {
	cases := []struct {
		name        string
		memoryKiB   uint32
		time        uint32
		parallelism uint8
	}{
		{"memory_too_low", 1, 1, 1},
		{"time_zero", 8, 0, 1},
		{"parallelism_zero", 8, 1, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Panics(t, func() {
				auth.NewArgon2idHasherWithParams(tc.memoryKiB, tc.time, tc.parallelism)
			})
		})
	}
}
