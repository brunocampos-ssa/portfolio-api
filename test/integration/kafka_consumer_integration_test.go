//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
)

// =============================================================================
// kafka.Consumer integration tests
// =============================================================================
//
// Three scenarios cover the consumer's full contract:
//
//   1. Round-trip delivery — every published event reaches the handler
//      exactly once (modulo at-least-once redelivery, which is the
//      handler's idempotency problem).
//
//   2. Bounded retry on handler error — a handler that fails twice
//      then succeeds is invoked exactly three times against the same
//      message; the offset is committed only after the success.
//
//   3. Consumer-group independence — the headline Kafka teaching test.
//      Two groups subscribed to the same topic each see the full
//      stream. Same producer, same topic, two completely independent
//      views — that's why event-persister, event-router, and
//      event-analytics can coexist without coordination.

// -----------------------------------------------------------------------------
// 1. Round-trip
// -----------------------------------------------------------------------------

func TestKafkaConsumer_DeliversAllPublishedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.consumer-roundtrip"
	createTopic(t, env.KafkaBrokers(), topic, 3)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	want := []*broker.EventEnvelope{
		newEnvelope("evt-rt-1", "w_alpha", "USDC", 100, now),
		newEnvelope("evt-rt-2", "w_beta", "USDT", 101, now.Add(time.Second)),
		newEnvelope("evt-rt-3", "w_alpha", "USDC", 102, now.Add(2*time.Second)),
		newEnvelope("evt-rt-4", "w_beta", "ETH", 103, now.Add(3*time.Second)),
	}
	for _, e := range want {
		require.NoError(t, pub.Publish(ctx, e))
	}
	require.NoError(t, pub.Close()) // flush

	cons, err := brokerkafka.NewConsumer(env.KafkaBrokers(), topic, "test-roundtrip-group")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cons.Close() })

	received := make(chan *broker.EventEnvelope, len(want))
	handler := func(_ context.Context, e *broker.EventEnvelope) error {
		received <- e
		return nil
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- cons.Run(runCtx, handler) }()

	got := make([]*broker.EventEnvelope, 0, len(want))
	for len(got) < len(want) {
		select {
		case e := <-received:
			got = append(got, e)
		case <-time.After(20 * time.Second):
			t.Fatalf("timeout: got %d/%d events", len(got), len(want))
		}
	}

	// Order is not guaranteed across partitions, only within a
	// partition. ElementsMatch handles set-equality.
	require.ElementsMatch(t, want, got)

	cancelRun()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("consumer Run returned unexpected err: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 2. Retry on handler error
// -----------------------------------------------------------------------------

func TestKafkaConsumer_RetriesHandlerErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	// Single partition keeps retry semantics easy to reason about —
	// the same Reader sees every message in publish order.
	topic := "wallet.events.test.consumer-retry"
	createTopic(t, env.KafkaBrokers(), topic, 1)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)

	wantEnv := newEnvelope("evt-retry-1", "w_retry", "USDC", 1, time.Now().UTC())
	require.NoError(t, pub.Publish(ctx, wantEnv))
	require.NoError(t, pub.Close())

	// Handler fails twice, succeeds on the third invocation. Asserts
	// that the consumer retries the SAME message rather than skipping
	// and that we don't keep retrying after success.
	var attempts atomic.Int32
	handler := func(_ context.Context, e *broker.EventEnvelope) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("transient failure on attempt %d", n)
		}
		return nil
	}

	cons, err := brokerkafka.NewConsumer(env.KafkaBrokers(), topic, "test-retry-group")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cons.Close() })

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- cons.Run(runCtx, handler) }()

	require.Eventually(t, func() bool {
		return attempts.Load() >= 3
	}, 20*time.Second, 50*time.Millisecond,
		"handler should be invoked exactly 3 times — 2 failures, 1 success")

	// Give the consumer time to commit and try to fetch the next
	// (non-existent) message. attempts should NOT exceed 3 — once the
	// handler succeeds, the message is committed and not retried.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int32(3), attempts.Load(),
		"consumer must not invoke handler again after success")

	cancelRun()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("consumer Run returned unexpected err: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 3. Consumer-group independence — the headline test.
// -----------------------------------------------------------------------------

// TestKafkaConsumer_GroupsHaveIndependentOffsets proves the headline
// Kafka semantic Class 2 leans on: two consumer groups subscribed to
// the same topic each receive the full stream, with completely
// independent offsets. This is what lets event-persister,
// event-router, and event-analytics coexist without any awareness of
// each other.
//
// Concretely: publish N messages, run a consumer in group "A" until
// it has all N, then run a fresh consumer in group "B" — also gets
// all N. If groups shared offsets, group B would see zero (group A
// already committed past everything).
func TestKafkaConsumer_GroupsHaveIndependentOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.consumer-groups"
	createTopic(t, env.KafkaBrokers(), topic, 3)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)

	const N = 6
	want := make([]*broker.EventEnvelope, 0, N)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < N; i++ {
		e := newEnvelope(
			fmt.Sprintf("evt-grp-%d", i),
			fmt.Sprintf("w_%d", i%2), // alternate wallets so partitions are exercised
			"USDC",
			uint64(100+i),
			now.Add(time.Duration(i)*time.Millisecond),
		)
		require.NoError(t, pub.Publish(ctx, e))
		want = append(want, e)
	}
	require.NoError(t, pub.Close())

	consumeAll := func(t *testing.T, groupID string) []*broker.EventEnvelope {
		t.Helper()
		c, err := brokerkafka.NewConsumer(env.KafkaBrokers(), topic, groupID)
		require.NoError(t, err)
		defer c.Close()

		ch := make(chan *broker.EventEnvelope, N)
		handler := func(_ context.Context, e *broker.EventEnvelope) error {
			ch <- e
			return nil
		}

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Run(runCtx, handler)
		}()

		out := make([]*broker.EventEnvelope, 0, N)
		for len(out) < N {
			select {
			case e := <-ch:
				out = append(out, e)
			case <-time.After(20 * time.Second):
				t.Fatalf("group %s timeout: got %d/%d", groupID, len(out), N)
			}
		}
		cancelRun()
		wg.Wait()
		return out
	}

	gotA := consumeAll(t, "test-groups-A")
	require.Len(t, gotA, N, "group A must receive every message")
	require.ElementsMatch(t, want, gotA)

	// Group B starts AFTER group A has fully consumed AND committed.
	// If groups shared offsets this would block forever (no new
	// messages past A's commit). It does NOT block — proof of the
	// independence property.
	gotB := consumeAll(t, "test-groups-B")
	require.Len(t, gotB, N, "group B must independently receive every message")
	require.ElementsMatch(t, want, gotB)
}
