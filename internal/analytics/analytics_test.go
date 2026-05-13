package analytics

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// TestApply_GroupsByNetworkDirectionToken verifies that the bucket key
// includes all four dimensions and that events with different keys do
// NOT collapse together. This is the core aggregation contract: every
// dashboard read depends on the buckets being orthogonal.
func TestApply_GroupsByNetworkDirectionToken(t *testing.T) {
	hour := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)

	agg := newAggregator(func() time.Time { return hour })
	ctx := context.Background()

	// Same hour, varying the other three dimensions.
	cases := []struct {
		network, direction, token string
		amount                    string
	}{
		{"ethereum", "incoming", "USDC", "100"},
		{"ethereum", "incoming", "USDC", "50"}, // same bucket → count 2, total 150
		{"ethereum", "outgoing", "USDC", "25"},
		{"ethereum", "incoming", "ETH", "1.5"},
		{"klever", "incoming", "USDC", "200"},
	}
	for _, c := range cases {
		require.NoError(t, agg.Apply(ctx, envWith(hour, c.network, c.direction, c.token, c.amount)))
	}

	snap := agg.Snapshot()
	require.Len(t, snap, 4, "expected 4 distinct buckets, got %d", len(snap))

	got := map[BucketKey]Summary{}
	for _, s := range snap {
		got[s.BucketKey] = s
	}

	usdcIncoming := got[BucketKey{"ethereum", "incoming", "USDC", hour}]
	require.Equal(t, uint64(2), usdcIncoming.Count, "two USDC incoming events should fold into one bucket")
	require.InDelta(t, 150.0, usdcIncoming.AmountTotal, 1e-9)

	require.Equal(t, uint64(1), got[BucketKey{"ethereum", "outgoing", "USDC", hour}].Count)
	require.Equal(t, uint64(1), got[BucketKey{"ethereum", "incoming", "ETH", hour}].Count)
	require.Equal(t, uint64(1), got[BucketKey{"klever", "incoming", "USDC", hour}].Count)
}

// TestApply_BucketsByHour pins down the time-windowing rule: events
// in different UTC hours land in different buckets, events within the
// same hour fold together regardless of minute/second.
func TestApply_BucketsByHour(t *testing.T) {
	h1 := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)
	h1Late := h1.Add(59 * time.Minute) // same hour bucket as h1
	h2 := h1.Add(time.Hour)
	h2Plus := h2.Add(30 * time.Minute) // same hour bucket as h2

	agg := newAggregator(func() time.Time { return h1 })
	ctx := context.Background()

	require.NoError(t, agg.Apply(ctx, envWith(h1, "ethereum", "incoming", "USDC", "10")))
	require.NoError(t, agg.Apply(ctx, envWith(h1Late, "ethereum", "incoming", "USDC", "20")))
	require.NoError(t, agg.Apply(ctx, envWith(h2, "ethereum", "incoming", "USDC", "30")))
	require.NoError(t, agg.Apply(ctx, envWith(h2Plus, "ethereum", "incoming", "USDC", "40")))

	snap := agg.Snapshot()
	require.Len(t, snap, 2, "expected exactly two hour-buckets")

	// Snapshot is sorted ascending by hour, so [0] is h1, [1] is h2.
	require.Equal(t, h1, snap[0].Hour, "first bucket must be h1")
	require.Equal(t, uint64(2), snap[0].Count)
	require.InDelta(t, 30.0, snap[0].AmountTotal, 1e-9)

	require.Equal(t, h2, snap[1].Hour, "second bucket must be h2")
	require.Equal(t, uint64(2), snap[1].Count)
	require.InDelta(t, 70.0, snap[1].AmountTotal, 1e-9)
}

// TestApply_ZeroEmittedAtFallsBackToClock guards the defensive branch
// where an envelope arrives with an unset EmittedAt. We don't want a
// year-0 bucket polluting dashboards, so the aggregator's clock fills
// in. Production envelopes always have EmittedAt set; this test makes
// the contract explicit.
func TestApply_ZeroEmittedAtFallsBackToClock(t *testing.T) {
	clock := time.Date(2026, 5, 13, 14, 30, 0, 0, time.UTC)
	agg := newAggregator(func() time.Time { return clock })

	env := envWith(time.Time{}, "ethereum", "incoming", "USDC", "10")
	require.NoError(t, agg.Apply(context.Background(), env))

	snap := agg.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, clock.Truncate(time.Hour), snap[0].Hour,
		"zero EmittedAt must fall back to clock's hour, not produce a year-0 bucket")
}

// TestApply_NilEnvelopeIsNoOp mirrors the persister/notifier nil-safety
// contract: defensive against a programmer error on the consumer
// side, never panics.
func TestApply_NilEnvelopeIsNoOp(t *testing.T) {
	agg := NewAggregator()
	require.NoError(t, agg.Apply(context.Background(), nil))
	require.Empty(t, agg.Snapshot())
}

// TestApply_UnparseableAmountDegradesToZero checks the
// log-and-continue policy for malformed amounts. The count still
// bumps (the event happened, after all) but the running total is
// unaffected. Analytics is a derived view, not a validator.
func TestApply_UnparseableAmountDegradesToZero(t *testing.T) {
	hour := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)
	agg := newAggregator(func() time.Time { return hour })

	require.NoError(t, agg.Apply(context.Background(),
		envWith(hour, "ethereum", "incoming", "USDC", "garbage")))
	require.NoError(t, agg.Apply(context.Background(),
		envWith(hour, "ethereum", "incoming", "USDC", "12.5")))

	snap := agg.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(2), snap[0].Count, "both events should count")
	require.InDelta(t, 12.5, snap[0].AmountTotal, 1e-9, "garbage amount counts as 0")
}

// TestApply_SafeForConcurrentUse stress-tests the mutex. If the lock
// were missing we'd expect either a race-detector failure or
// non-deterministic counts.
//
// Run with `go test -race ./internal/analytics/...` to catch the
// lock-omission regression directly.
func TestApply_SafeForConcurrentUse(t *testing.T) {
	hour := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)
	agg := newAggregator(func() time.Time { return hour })

	const goroutines = 8
	const perGoroutine = 250

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				_ = agg.Apply(context.Background(),
					envWith(hour, "ethereum", "incoming", "USDC", "1"))
			}
		})
	}
	wg.Wait()

	snap := agg.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(goroutines*perGoroutine), snap[0].Count,
		"every Apply must contribute one count under concurrent load")
	require.InDelta(t, float64(goroutines*perGoroutine), snap[0].AmountTotal, 1e-9)
}

// TestDump_StableOrderAndFormat freezes the on-the-wire dump output
// so a future format tweak forces a deliberate test update. Operators
// (and downstream log scrapers) get the same shape every run.
func TestDump_StableOrderAndFormat(t *testing.T) {
	h1 := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)
	h2 := h1.Add(time.Hour)
	agg := newAggregator(func() time.Time { return h1 })
	ctx := context.Background()

	// Intentionally insert out of sort order — Dump must sort.
	require.NoError(t, agg.Apply(ctx, envWith(h2, "klever", "incoming", "KLV", "5")))
	require.NoError(t, agg.Apply(ctx, envWith(h1, "ethereum", "outgoing", "ETH", "1")))
	require.NoError(t, agg.Apply(ctx, envWith(h1, "ethereum", "incoming", "USDC", "100")))
	require.NoError(t, agg.Apply(ctx, envWith(h1, "ethereum", "incoming", "USDC", "50")))

	var buf bytes.Buffer
	_, err := agg.Dump(&buf)
	require.NoError(t, err)

	// 4 events but only 3 distinct buckets — the two h1/USDC/incoming
	// events fold into a single bucket with count=2.
	want := strings.Join([]string{
		"analytics: dump buckets=3",
		"analytics: bucket window=2026-05-13T14:00:00Z network=ethereum direction=incoming token=USDC count=2 amount_total=150.000000",
		"analytics: bucket window=2026-05-13T14:00:00Z network=ethereum direction=outgoing token=ETH count=1 amount_total=1.000000",
		"analytics: bucket window=2026-05-13T15:00:00Z network=klever direction=incoming token=KLV count=1 amount_total=5.000000",
		"",
	}, "\n")
	require.Equal(t, want, buf.String())
}

// TestDump_EmptyEmitsHeartbeat verifies the always-present header
// even with zero buckets — the binary's "I'm alive" signal on an
// idle topic.
func TestDump_EmptyEmitsHeartbeat(t *testing.T) {
	agg := NewAggregator()
	var buf bytes.Buffer
	_, err := agg.Dump(&buf)
	require.NoError(t, err)
	require.Equal(t, "analytics: dump buckets=0\n", buf.String())
}

// TestSnapshot_NotNilOnEmpty pins the "empty slice, not nil" contract
// so callers can range without a nil-check. Cheap test, prevents a
// future "return nil for empty" optimisation that would break callers.
func TestSnapshot_NotNilOnEmpty(t *testing.T) {
	agg := NewAggregator()
	snap := agg.Snapshot()
	require.NotNil(t, snap, "Snapshot must return an empty slice, not nil")
	require.Empty(t, snap)
}

func envWith(emittedAt time.Time, network, direction, token, amount string) *broker.EventEnvelope {
	return &broker.EventEnvelope{
		EventID:         "evt-" + network + "-" + direction + "-" + token + "-" + amount,
		SchemaVersion:   broker.SchemaCurrent,
		Network:         network,
		EventType:       "transfer",
		Direction:       direction,
		WalletID:        "w_1",
		TokenSymbol:     token,
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          amount,
		TxHash:          "0xdeadbeef",
		BlockNumber:     100,
		EmittedAt:       emittedAt,
	}
}
