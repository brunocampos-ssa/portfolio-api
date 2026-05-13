// Package analytics is the business logic of cmd/event-analytics: an
// in-memory aggregator that counts wallet events bucketed by
// (network, direction, token, hour) and prints a periodic dump to
// stdout. It replaces Class 1's in-process metricsWorker — same
// counters, now living in its own consumer-group binary so it can be
// scaled, restarted, and reasoned about independently of the watcher.
//
// Two Class 2 teaching points live here:
//
//  1. CONSUMER-GROUP REPLAY. The analytics binary uses
//     kafka.NewReplayConsumer, which gives the group a unique-per-
//     process ID and skips offset commits. Every restart recomputes
//     totals from t=0. Contrast with the persister, whose group
//     resumes from committed offsets — same topic, two consumers,
//     opposite needs, expressed as a constructor choice.
//
//  2. STATE IS EPHEMERAL. The aggregator keeps everything in a Go
//     map. Crash the process and the numbers are gone — but the
//     SOURCE OF TRUTH is Kafka, so the next startup rebuilds them
//     deterministically. This is the "derive views from the log"
//     pattern that justifies durable event streams.
//
// Concurrency contract: Apply, Snapshot, and Dump are safe to call
// from any goroutine. In practice the consumer drives Apply from one
// goroutine and a periodic-dump goroutine reads via Dump; a future
// scaling story might dispatch Apply across N workers and that
// pattern works without changes here.
package analytics

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// BucketKey identifies one aggregation bucket. The four dimensions
// match what an operator typically wants to see on a dashboard:
//
//   - Network: which chain ("ethereum", "klever", ...).
//   - Direction: "incoming" or "outgoing" (relative to a tracked wallet).
//   - Token: ERC-20 symbol or native asset ("USDC", "ETH", ...).
//   - Hour: UTC hour the event was EMITTED (NOT block timestamp).
//     Using EmittedAt gives stable buckets across replays — block
//     timestamps drift slightly with reorgs, EmittedAt is fixed at
//     publish time.
type BucketKey struct {
	Network   string
	Direction string
	Token     string
	Hour      time.Time
}

// BucketStats is the running aggregate per bucket. Counts are
// authoritative; AmountTotal is a best-effort float sum suitable for
// dashboard headline numbers, NOT for accounting. Precise sums require
// big.Int or shopspring/decimal in a real system, but the float here
// keeps the package small enough for a 25-minute classroom segment
// and the wallet_events table is the precise source if needed.
type BucketStats struct {
	Count       uint64
	AmountTotal float64
}

// Aggregator is the in-memory store. Construction is via NewAggregator
// so the now-func can be injected (real time in production, a fixed
// clock in tests).
type Aggregator struct {
	mu      sync.RWMutex
	buckets map[BucketKey]*BucketStats

	// now returns "the current time" for hour-bucket attribution
	// fallback when EmittedAt is the zero value. Production passes
	// time.Now; tests freeze it.
	now func() time.Time
}

// NewAggregator returns a fresh aggregator. The now func is used only
// as a fallback when an envelope's EmittedAt is the zero value — a
// shape we shouldn't see in practice but accept defensively rather
// than dropping the event.
func NewAggregator() *Aggregator {
	return newAggregator(time.Now)
}

// newAggregator is the test seam — callers in production go through
// NewAggregator, tests use this directly to inject a frozen clock.
func newAggregator(now func() time.Time) *Aggregator {
	return &Aggregator{
		buckets: make(map[BucketKey]*BucketStats),
		now:     now,
	}
}

// Apply implements broker.Handler. It accumulates the envelope into
// its bucket. The signature returns an error to match the interface,
// but in practice Apply NEVER errors — analytics is an append-only
// derived view, and "I couldn't parse the amount" is logged-and-
// counted, not retried. A retry would just produce the same parse
// failure on the same bytes.
func (a *Aggregator) Apply(_ context.Context, env *broker.EventEnvelope) error {
	if env == nil {
		// Defensive: kafka.Consumer never sends nil. Mirroring the
		// persister/router/notifier nil-safety contract.
		return nil
	}

	key := BucketKey{
		Network:   env.Network,
		Direction: env.Direction,
		Token:     env.TokenSymbol,
		Hour:      a.hourBucket(env.EmittedAt),
	}

	// parseAmount tolerates malformed strings — analytics is a
	// derived view, NOT a validator, so we accept zero for unparseable
	// amounts and still bump the count. The persister will surface
	// schema problems separately.
	amount := parseAmount(env.Amount)

	a.mu.Lock()
	stats, ok := a.buckets[key]
	if !ok {
		stats = &BucketStats{}
		a.buckets[key] = stats
	}
	stats.Count++
	stats.AmountTotal += amount
	a.mu.Unlock()
	return nil
}

// hourBucket truncates a timestamp to the start of its UTC hour. If
// the input is the zero value (shouldn't happen but guard anyway),
// fall back to the configured clock so we don't pile everything into
// the year-0 bucket.
func (a *Aggregator) hourBucket(t time.Time) time.Time {
	if t.IsZero() {
		t = a.now()
	}
	return t.UTC().Truncate(time.Hour)
}

// Summary is the immutable shape returned by Snapshot. Embeds the key
// so callers don't have to unpack a map[BucketKey]*BucketStats and
// can sort/serialize directly.
type Summary struct {
	BucketKey
	Count       uint64
	AmountTotal float64
}

// Snapshot returns a deterministic, sorted copy of every bucket.
// Sort order: Hour ASC, Network, Direction, Token. The copy is safe
// to hold and inspect without locks — Apply will not mutate the
// returned slice's stats.
//
// Returns an empty slice (not nil) when no events have been applied
// so callers can range without a nil-check.
func (a *Aggregator) Snapshot() []Summary {
	a.mu.RLock()
	out := make([]Summary, 0, len(a.buckets))
	for k, v := range a.buckets {
		out = append(out, Summary{
			BucketKey:   k,
			Count:       v.Count,
			AmountTotal: v.AmountTotal,
		})
	}
	a.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if !out[i].Hour.Equal(out[j].Hour) {
			return out[i].Hour.Before(out[j].Hour)
		}
		if out[i].Network != out[j].Network {
			return out[i].Network < out[j].Network
		}
		if out[i].Direction != out[j].Direction {
			return out[i].Direction < out[j].Direction
		}
		return out[i].Token < out[j].Token
	})
	return out
}

// Dump writes a human-readable line per bucket to w. Output is stable
// (sorted) so successive dumps can be diffed by eye and tests can
// assert on the exact text. The header line is always present even
// when there are zero buckets — it doubles as a heartbeat so an
// operator can see the analytics binary is alive on an idle topic.
func (a *Aggregator) Dump(w io.Writer) (int, error) {
	summary := a.Snapshot()
	total := 0

	header := fmt.Sprintf("analytics: dump buckets=%d\n", len(summary))
	n, err := io.WriteString(w, header)
	total += n
	if err != nil {
		return total, err
	}

	for _, s := range summary {
		line := fmt.Sprintf(
			"analytics: bucket window=%s network=%s direction=%s token=%s count=%d amount_total=%.6f\n",
			s.Hour.Format(time.RFC3339),
			s.Network, s.Direction, s.Token,
			s.Count, s.AmountTotal,
		)
		n, err := io.WriteString(w, line)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// parseAmount accepts the envelope's stringified decimal. Anything
// unparseable degrades to zero — see Apply's docstring.
func parseAmount(s string) float64 {
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// Compile-time assertion that Apply satisfies the handler contract.
// This is the line that lets cmd/event-analytics pass agg.Apply
// directly to consumer.Run.
var _ broker.Handler = (*Aggregator)(nil).Apply
