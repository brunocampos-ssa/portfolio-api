package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// =============================================================================
// argon2id password hashing — PHC string format
// =============================================================================
//
// This package implements password hashing using argon2id, the Password-
// Hashing Competition winner and the algorithm OWASP recommends as of 2024.
//
// argon2id is a memory-hard function: cracking it requires both CPU time and
// large amounts of RAM. That's a deliberate defense against GPU/ASIC attacks,
// which dominate against bcrypt/SHA-based hashes. The trade-off is three
// tunable parameters:
//
//   memory       (KiB) — RAM the hash function allocates. Higher is harder to
//                        parallelise on attacker hardware. OWASP suggests 19 MiB
//                        as a floor; we use 64 MiB.
//   time         (n)   — number of passes over the memory. Linear cost.
//   parallelism  (p)   — threads. We use 2 — server cost stays bounded while
//                        the parameter is published in the hash itself, so
//                        verification is reproducible everywhere.
//
// We store hashes in the standard PHC string format
// (https://github.com/P-H-C/phc-string-format) so a future migration to
// different parameters or algorithms can read existing hashes by inspection:
//
//	$argon2id$v=19$m=65536,t=3,p=2$<salt-b64>$<hash-b64>
//
// Why bother with the PHC format instead of a custom struct? Two reasons:
// it's portable across languages (a Python or Rust verifier can read it),
// and it carries the parameters with the hash, so we can roll parameters
// without breaking existing users.

// Default argon2id parameters. Tuned for ~50–150 ms verification on a
// developer laptop — comfortable for an interactive login, painful for an
// attacker grinding billions of guesses.
const (
	defaultMemoryKiB    uint32 = 64 * 1024 // 64 MiB
	defaultTime         uint32 = 3
	defaultParallelism  uint8  = 2
	defaultSaltLenBytes uint32 = 16
	defaultKeyLenBytes  uint32 = 32
)

// argon2Params bundles the per-hash configuration we encode into the PHC string.
type argon2Params struct {
	memoryKiB   uint32
	time        uint32
	parallelism uint8
	saltLen     uint32
	keyLen      uint32
}

// Argon2idHasher implements password hashing with argon2id and PHC encoding.
//
// Defensive programming: callers should construct via NewArgon2idHasher so
// tests and production share the same parameter floor; zero-value usage is
// blocked by the constructor's validation.
type Argon2idHasher struct {
	params argon2Params
}

// NewArgon2idHasher returns a hasher with OWASP-leaning defaults.
func NewArgon2idHasher() *Argon2idHasher {
	return &Argon2idHasher{
		params: argon2Params{
			memoryKiB:   defaultMemoryKiB,
			time:        defaultTime,
			parallelism: defaultParallelism,
			saltLen:     defaultSaltLenBytes,
			keyLen:      defaultKeyLenBytes,
		},
	}
}

// NewArgon2idHasherWithParams allows callers (typically tests) to override
// the cost parameters. The constructor refuses obviously-broken values so a
// typo in a test fixture does not silently weaken production hashes.
func NewArgon2idHasherWithParams(memoryKiB, time uint32, parallelism uint8) *Argon2idHasher {
	if memoryKiB < 8 {
		panic("auth.NewArgon2idHasherWithParams: memoryKiB must be >= 8")
	}
	if time < 1 {
		panic("auth.NewArgon2idHasherWithParams: time must be >= 1")
	}
	if parallelism < 1 {
		panic("auth.NewArgon2idHasherWithParams: parallelism must be >= 1")
	}
	return &Argon2idHasher{
		params: argon2Params{
			memoryKiB:   memoryKiB,
			time:        time,
			parallelism: parallelism,
			saltLen:     defaultSaltLenBytes,
			keyLen:      defaultKeyLenBytes,
		},
	}
}

// Hash returns the PHC-encoded argon2id hash of the given password.
//
// A fresh random salt is generated for every call; identical passwords
// produce different hashes. Comparison goes through Verify, which decodes
// the parameters and salt from the stored hash.
func (h *Argon2idHasher) Hash(password string) (string, error) {
	if password == "" {
		return "", errors.New("auth: password must not be empty")
	}

	salt := make([]byte, h.params.saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: read salt: %w", err)
	}

	key := argon2.IDKey(
		[]byte(password),
		salt,
		h.params.time,
		h.params.memoryKiB,
		h.params.parallelism,
		h.params.keyLen,
	)

	return encodePHC(h.params, salt, key), nil
}

// Verify reports whether the given password matches the stored PHC hash.
//
// The function is constant-time across the hash comparison so an attacker
// cannot use timing side-channels to learn anything about the stored value.
// It is NOT constant-time across malformed inputs — but those happen on the
// developer side (corrupt DB, wrong column), not on attacker-controlled data.
func (h *Argon2idHasher) Verify(encodedHash, password string) (bool, error) {
	if encodedHash == "" || password == "" {
		return false, nil
	}

	params, salt, expected, err := decodePHC(encodedHash)
	if err != nil {
		return false, fmt.Errorf("auth: decode hash: %w", err)
	}

	candidate := argon2.IDKey(
		[]byte(password),
		salt,
		params.time,
		params.memoryKiB,
		params.parallelism,
		uint32(len(expected)),
	)

	if subtle.ConstantTimeCompare(candidate, expected) == 1 {
		return true, nil
	}
	return false, nil
}

// =============================================================================
// PHC string format (encode/decode)
// =============================================================================
//
// Format: $argon2id$v=19$m=<m>,t=<t>,p=<p>$<salt-b64>$<hash-b64>
//
// We use base64 with NO padding (RFC 4648 §3.2), as required by PHC.

func encodePHC(p argon2Params, salt, key []byte) string {
	saltB64 := base64.RawStdEncoding.EncodeToString(salt)
	keyB64 := base64.RawStdEncoding.EncodeToString(key)
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		p.memoryKiB, p.time, p.parallelism,
		saltB64, keyB64,
	)
}

// decodePHC parses a PHC string and returns the params, salt, and key bytes.
//
// Strict validation: any deviation from the documented format is an error.
// We do NOT silently accept hashes with unknown algorithms or missing fields.
func decodePHC(s string) (argon2Params, []byte, []byte, error) {
	parts := strings.Split(s, "$")
	// Expected layout: ["", "argon2id", "v=19", "m=...,t=...,p=...", "<salt>", "<key>"]
	if len(parts) != 6 {
		return argon2Params{}, nil, nil, fmt.Errorf("malformed PHC: expected 6 fields, got %d", len(parts))
	}
	if parts[1] != "argon2id" {
		return argon2Params{}, nil, nil, fmt.Errorf("unsupported algorithm: %q", parts[1])
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return argon2Params{}, nil, nil, fmt.Errorf("malformed version field: %w", err)
	}
	if version != argon2.Version {
		return argon2Params{}, nil, nil, fmt.Errorf("unsupported argon2 version: %d", version)
	}

	var p argon2Params
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memoryKiB, &p.time, &p.parallelism); err != nil {
		return argon2Params{}, nil, nil, fmt.Errorf("malformed params field: %w", err)
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return argon2Params{}, nil, nil, fmt.Errorf("decode salt: %w", err)
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return argon2Params{}, nil, nil, fmt.Errorf("decode key: %w", err)
	}

	p.saltLen = uint32(len(salt))
	p.keyLen = uint32(len(key))
	return p, salt, key, nil
}
