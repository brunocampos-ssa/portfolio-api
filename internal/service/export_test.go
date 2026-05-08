package service

import "time"

// =============================================================================
// Test-only exports.
//
// This file is only compiled during `go test`. It lets tests in the
// external test package (`service_test`) reach private fields that we
// deliberately keep unexported in production — most notably the clock
// and id generator on AuthService.
//
// Pattern: prefer this over making the fields public. The production API
// stays minimal and intentional, while tests get the hooks they need.
// =============================================================================

// SetClock replaces the AuthService clock so tests can drive time forward
// deterministically. Returns the original clock so the caller can restore.
func SetClock(s *AuthService, fn func() time.Time) func() time.Time {
	prev := s.now
	s.now = fn
	return prev
}

// SetIDGen replaces the AuthService id generator so tests can produce
// stable IDs like "id-1", "id-2" rather than random hex.
func SetIDGen(s *AuthService, fn func() string) func() string {
	prev := s.idGen
	s.idGen = fn
	return prev
}
