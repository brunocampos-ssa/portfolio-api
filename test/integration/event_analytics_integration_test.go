//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/analytics"
	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
)

// =============================================================================
// event-analytics integration tests
// =============================================================================
//
// Two scenarios, both built against a real testcontainers Kafka:
//
//   1. Aggregation correctness across all four bucket dimensions
//      (network, direction, token, hour). Publishes a hand-picked
//      mix of envelopes, runs the replay consumer, and asserts the
//      Snapshot folds them into the expected buckets with the right
//      counts and amount totals.
//
//   2. Replay-from-zero on restart — the headline test of the
//      consumer-group teaching point. We:
//        a) publish N events,
//        b) start the analytics consumer, wait for it to see all N,
//        c) close it,
//        d) start a SECOND analytics consumer against the same base
//           group ID, and assert it ALSO sees all N — not zero, not
//           any subset.
//      This proves the per-process unique group ID + no-commit
//      recipe in kafka.NewReplayConsumer delivers the contract the
//      analytics binary promises: every restart recomputes totals
//      from t=0.

// -----------------------------------------------------------------------------
// 1. Aggregation correctness
// -----------------------------------------------------------------------------

func TestEventAnalytics_AggregatesAcrossDimensions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.analytics-buckets"
	createTopic(t, env.KafkaBrokers(), topic, 3)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Two hour windows so we exercise the time dimension too.
	h1 := time.Now().UTC().Truncate(time.Hour)
	h2 := h1.Add(time.Hour)

	// Hand-built envelopes designed to:
	//   - fold 2 events into one (h1/ethereum/incoming/USDC),
	//   - distinguish by direction (outgoing USDC, same hour),
	//   - distinguish by token (ETH, same hour/network/direction),
	//   - distinguish by network (klever, same hour/direction/token),
	//   - distinguish by hour (h2 USDC incoming).
	publishes := []struct {
		eventID, network, direction, token, amount string
		emitted                                    time.Time
	}{
		{"evt-1", "ethereum", "incoming", "USDC", "100", h1},
		{"evt-2", "ethereum", "incoming", "USDC", "50", h1.Add(time.Minute)},
		{"evt-3", "ethereum", "outgoing", "USDC", "25", h1.Add(2 * time.Minute)},
		{"evt-4", "ethereum", "incoming", "ETH", "1.5", h1.Add(3 * time.Minute)},
		{"evt-5", "klever", "incoming", "USDC", "200", h1.Add(4 * time.Minute)},
		{"evt-6", "ethereum", "incoming", "USDC", "10", h2},
	}
	for _, p := range publishes {
		e := newEnvelope(p.eventID, "w_x", p.token, 1, p.emitted)
		e.Network = p.network
		e.Direction = p.direction
		e.Amount = p.amount
		require.NoError(t, pub.Publish(ctx, e))
	}

	// Start the analytics replay consumer. The base group ID below
	// gets a unique nanos suffix appended internally, so we can run
	// many of these tests in parallel without offset collisions.
	consumer, err := brokerkafka.NewReplayConsumer(
		env.KafkaBrokers(), topic, "wallet-events-analytics-test-buckets",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	agg := analytics.NewAggregator()

	// Wrap Apply with an atomic counter so we know when all N events
	// have been processed without polling the (lock-guarded) snapshot
	// on a hot path.
	var processed atomic.Int64
	handler := func(ctx context.Context, e *broker.EventEnvelope) error {
		err := agg.Apply(ctx, e)
		if err == nil {
			processed.Add(1)
		}
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx, handler) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case e := <-done:
			if e != nil && !errors.Is(e, context.Canceled) {
				t.Logf("analytics consumer exit: %v", e)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("analytics consumer did not exit within 5s of cancel")
		}
	})

	// Wait until every published envelope has been Apply'd.
	require.Eventually(t, func() bool {
		return processed.Load() == int64(len(publishes))
	}, 30*time.Second, 100*time.Millisecond,
		"replay consumer must process all %d published events", len(publishes))

	snap := agg.Snapshot()

	// Index the snapshot by key for cleaner assertions.
	byKey := map[analytics.BucketKey]analytics.Summary{}
	for _, s := range snap {
		byKey[s.BucketKey] = s
	}

	usdcInH1 := byKey[analytics.BucketKey{Network: "ethereum", Direction: "incoming", Token: "USDC", Hour: h1}]
	require.Equal(t, uint64(2), usdcInH1.Count, "2 USDC incoming events in h1 should fold")
	require.InDelta(t, 150.0, usdcInH1.AmountTotal, 1e-9)

	usdcOutH1 := byKey[analytics.BucketKey{Network: "ethereum", Direction: "outgoing", Token: "USDC", Hour: h1}]
	require.Equal(t, uint64(1), usdcOutH1.Count, "USDC outgoing must NOT fold with USDC incoming")
	require.InDelta(t, 25.0, usdcOutH1.AmountTotal, 1e-9)

	ethInH1 := byKey[analytics.BucketKey{Network: "ethereum", Direction: "incoming", Token: "ETH", Hour: h1}]
	require.Equal(t, uint64(1), ethInH1.Count, "ETH must NOT fold with USDC")
	require.InDelta(t, 1.5, ethInH1.AmountTotal, 1e-9)

	klvInH1 := byKey[analytics.BucketKey{Network: "klever", Direction: "incoming", Token: "USDC", Hour: h1}]
	require.Equal(t, uint64(1), klvInH1.Count, "klever USDC must NOT fold with ethereum USDC")
	require.InDelta(t, 200.0, klvInH1.AmountTotal, 1e-9)

	usdcInH2 := byKey[analytics.BucketKey{Network: "ethereum", Direction: "incoming", Token: "USDC", Hour: h2}]
	require.Equal(t, uint64(1), usdcInH2.Count, "h2 must NOT fold with h1")
	require.InDelta(t, 10.0, usdcInH2.AmountTotal, 1e-9)

	require.Len(t, snap, 5, "expected exactly 5 distinct buckets, got %d: %+v", len(snap), snap)
}

// -----------------------------------------------------------------------------
// 2. Replay-from-zero on every (re)start
// -----------------------------------------------------------------------------

func TestEventAnalytics_RestartReplaysFromZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.analytics-replay"
	createTopic(t, env.KafkaBrokers(), topic, 3)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Publish N envelopes BEFORE either consumer starts. This rules
	// out "consumer A and B saw different live streams" and locks the
	// test to the replay property.
	const N = 8
	now := time.Now().UTC().Truncate(time.Millisecond)
	wantIDs := make(map[string]struct{}, N)
	for i := range N {
		eventID := fmt.Sprintf("evt-replay-%d", i)
		wantIDs[eventID] = struct{}{}
		require.NoError(t, pub.Publish(ctx,
			newEnvelope(eventID, fmt.Sprintf("w_%d", i%2), "USDC", uint64(100+i),
				now.Add(time.Duration(i)*time.Millisecond))))
	}

	// runOnce starts a fresh replay consumer, waits for it to process
	// exactly N events, captures the seen EventIDs, and tears it down.
	// Each call MUST observe all N — that's the property under test.
	runOnce := func(label string) map[string]struct{} {
		consumer, err := brokerkafka.NewReplayConsumer(
			env.KafkaBrokers(), topic, "wallet-events-analytics-test-replay",
		)
		require.NoError(t, err)

		seen := make(chan string, N*2)
		handler := func(_ context.Context, e *broker.EventEnvelope) error {
			seen <- e.EventID
			return nil
		}

		runCtx, cancelRun := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- consumer.Run(runCtx, handler) }()

		got := make(map[string]struct{}, N)
		for len(got) < N {
			select {
			case id := <-seen:
				got[id] = struct{}{}
			case <-time.After(30 * time.Second):
				cancelRun()
				_ = consumer.Close()
				t.Fatalf("%s: replay timeout — got %d/%d events", label, len(got), N)
			}
		}

		cancelRun()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("%s: consumer did not exit within 5s of cancel", label)
		}
		_ = consumer.Close()
		return got
	}

	gotA := runOnce("first run")
	require.Equal(t, wantIDs, gotA,
		"first analytics run must see every published event")

	// The decisive assertion: a second run, against the SAME base
	// group ID, also sees every event from offset 0. If the consumer
	// were resuming from committed offsets (as the persister does)
	// this would block forever waiting for the (N+1)th event.
	gotB := runOnce("restart")
	require.Equal(t, wantIDs, gotB,
		"restart must replay from t=0 — every event re-delivered, not just messages since last commit")
}
