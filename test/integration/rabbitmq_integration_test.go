//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerrabbit "github.com/brunocampos-ssa/portfolio-api/internal/broker/rabbitmq"
)

// =============================================================================
// rabbitmq.Publisher + rabbitmq.Consumer integration tests
// =============================================================================
//
// Four scenarios cover the full RabbitMQ adapter contract:
//
//   1. Round-trip — publish N envelopes, consumer with binding "#"
//      receives all of them. Headline test that the publisher confirms
//      and the consumer's ack flow both work end-to-end.
//
//   2. Routing-key filtering — the headline RabbitMQ teaching test.
//      Bind one queue with "*.incoming.*" and another with "*.*.usdc".
//      Publish a mix; assert each queue receives only what its
//      pattern matches. Same exchange, two slices of the firehose.
//
//   3. Bounded handler retry — handler fails twice, succeeds on third
//      attempt. Asserts attempts==3 and only one Ack happens. Mirrors
//      kafka.Consumer's retry test so the cross-broker symmetry is
//      visible.
//
//   4. Closed publisher rejects further Publish calls.
//
// Each test uses a unique queue name (test-name + run nanos) so
// re-runs and parallel-ish tests on the shared testenv RabbitMQ
// never see each other's accumulated bindings.

const exchangeUnderTest = broker.RabbitExchangeWalletEvents

// uniqueQueue derives a queue name from the test name plus a
// nanosecond timestamp. Each invocation produces a fresh queue, so we
// never inherit bindings from a previous run — that was the bug that
// caused the routing-key test to silently match too many messages
// during development.
func uniqueQueue(t *testing.T, label string) string {
	t.Helper()
	return fmt.Sprintf("test-rabbit-%s-%d", label, time.Now().UnixNano())
}

// runConsumer starts c.Run in a goroutine and registers a t.Cleanup
// that cancels the run-context and waits for the goroutine to exit
// BEFORE Close is called on the consumer. This order matters: amqp091
// channels are not safe for concurrent use, so closing the channel
// while Run is still doing ack/nack can deadlock the broker RPC. The
// cleanup is registered before the consumer.Close cleanup so it runs
// LATER in LIFO order — i.e., first wait for goroutine to exit, then
// (when consumer.Close fires next) close the channel.
//
// Returns the cancel func so the test can cancel early if needed.
func runConsumer(t *testing.T, c *brokerrabbit.Consumer, parent context.Context, handler broker.Handler) context.CancelFunc {
	t.Helper()
	runCtx, cancelRun := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		_ = c.Run(runCtx, handler)
		close(done)
	}()
	t.Cleanup(func() {
		cancelRun()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("consumer Run did not exit within 5s of cancel")
		}
	})
	return cancelRun
}

// -----------------------------------------------------------------------------
// 1. Round-trip
// -----------------------------------------------------------------------------

func TestRabbitMQ_PublishConsume_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.RabbitURL())

	pub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	queueName := uniqueQueue(t, "roundtrip")
	cons, err := brokerrabbit.NewConsumer(env.RabbitURL(), exchangeUnderTest, queueName, "#")
	require.NoError(t, err)
	// IMPORTANT: register the Close cleanup BEFORE runConsumer so the
	// run-wait cleanup runs FIRST in LIFO order. See runConsumer's
	// docstring for the rationale.
	t.Cleanup(func() { _ = cons.Close() })

	received := make(chan *broker.EventEnvelope, 8)
	handler := func(_ context.Context, e *broker.EventEnvelope) error {
		received <- e
		return nil
	}
	runConsumer(t, cons, ctx, handler)

	want := []*broker.EventEnvelope{
		newEnvelope("evt-rabbit-rt-1", "w_a", "USDC", 1, time.Now().UTC()),
		newEnvelope("evt-rabbit-rt-2", "w_a", "USDT", 2, time.Now().UTC()),
		newEnvelope("evt-rabbit-rt-3", "w_b", "ETH", 3, time.Now().UTC()),
	}
	for _, e := range want {
		require.NoError(t, pub.Publish(ctx, e), "publish %s", e.EventID)
	}

	got := make([]*broker.EventEnvelope, 0, len(want))
	for len(got) < len(want) {
		select {
		case e := <-received:
			got = append(got, e)
		case <-time.After(15 * time.Second):
			t.Fatalf("timeout: got %d/%d", len(got), len(want))
		}
	}
	require.ElementsMatch(t, want, got)
}

// -----------------------------------------------------------------------------
// 2. Routing-key filtering — the headline RabbitMQ test.
// -----------------------------------------------------------------------------

// TestRabbitMQ_RoutingKeysFilterIndependently proves that distinct
// queues bound to the same exchange with different routing-key
// patterns each receive only the slice of the stream they asked for.
//
// We publish a mix of (direction × token) combinations and assert:
//   - the "*.incoming.*" queue gets every incoming event regardless of token
//   - the "*.*.usdc" queue gets every USDC event regardless of direction
//   - their intersection (incoming USDC) is what an event-notifier
//     subscribed to "*.incoming.usdc" would see
//
// Doubles as documentation of which AMQP topic-patterns mean what:
// "*" matches one segment, "#" matches zero or more.
func TestRabbitMQ_RoutingKeysFilterIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	require.NotEmpty(t, env.RabbitURL())

	pub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	consIncoming, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), exchangeUnderTest,
		uniqueQueue(t, "incoming"), "*.incoming.*",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consIncoming.Close() })

	consUSDC, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), exchangeUnderTest,
		uniqueQueue(t, "usdc"), "*.*.usdc",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consUSDC.Close() })

	gotIncoming := make(chan *broker.EventEnvelope, 8)
	gotUSDC := make(chan *broker.EventEnvelope, 8)

	runConsumer(t, consIncoming, ctx, func(_ context.Context, e *broker.EventEnvelope) error {
		gotIncoming <- e
		return nil
	})
	runConsumer(t, consUSDC, ctx, func(_ context.Context, e *broker.EventEnvelope) error {
		gotUSDC <- e
		return nil
	})

	// Build an envelope with explicit (direction, token) so the
	// routing-key segments are predictable. The default helper
	// (newEnvelope) always uses incoming/USDC.
	mk := func(eventID, direction, token string) *broker.EventEnvelope {
		e := newEnvelope(eventID, "w_x", token, 1, time.Now().UTC())
		e.Direction = direction
		return e
	}
	all := []*broker.EventEnvelope{
		mk("rk-1", "incoming", "USDC"), // matches BOTH queues
		mk("rk-2", "incoming", "USDT"), // matches incoming only
		mk("rk-3", "outgoing", "USDC"), // matches usdc only
		mk("rk-4", "outgoing", "ETH"),  // matches NEITHER
	}
	wantIncoming := []*broker.EventEnvelope{all[0], all[1]}
	wantUSDC := []*broker.EventEnvelope{all[0], all[2]}

	for _, e := range all {
		require.NoError(t, pub.Publish(ctx, e), "publish %s rk=%s", e.EventID, broker.RoutingKey(e))
	}

	collect := func(t *testing.T, ch <-chan *broker.EventEnvelope, n int, label string) []*broker.EventEnvelope {
		t.Helper()
		out := make([]*broker.EventEnvelope, 0, n)
		for len(out) < n {
			select {
			case e := <-ch:
				out = append(out, e)
			case <-time.After(15 * time.Second):
				t.Fatalf("%s: timeout, got %d/%d", label, len(out), n)
			}
		}
		return out
	}

	gotIncomingList := collect(t, gotIncoming, len(wantIncoming), "incoming queue")
	gotUSDCList := collect(t, gotUSDC, len(wantUSDC), "usdc queue")

	require.ElementsMatch(t, wantIncoming, gotIncomingList,
		"*.incoming.* queue must receive both incoming events and ONLY those")
	require.ElementsMatch(t, wantUSDC, gotUSDCList,
		"*.*.usdc queue must receive both USDC events and ONLY those")

	// Give the broker a beat to deliver any stragglers (none expected
	// — the rk-4 outgoing.eth case is bound by neither pattern).
	time.Sleep(500 * time.Millisecond)
	require.Empty(t, gotIncoming, "no extra messages after the expected count")
	require.Empty(t, gotUSDC, "no extra messages after the expected count")
}

// -----------------------------------------------------------------------------
// 3. Bounded retry on handler error.
// -----------------------------------------------------------------------------

func TestRabbitMQConsumer_RetriesHandlerErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.RabbitURL())

	pub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	queueName := uniqueQueue(t, "retry")
	cons, err := brokerrabbit.NewConsumer(env.RabbitURL(), exchangeUnderTest, queueName, "#")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cons.Close() })

	wantEnv := newEnvelope("evt-rabbit-retry", "w_retry", "USDC", 1, time.Now().UTC())
	require.NoError(t, pub.Publish(ctx, wantEnv))

	var attempts atomic.Int32
	runConsumer(t, cons, ctx, func(_ context.Context, e *broker.EventEnvelope) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("transient failure on attempt %d", n)
		}
		return nil
	})

	require.Eventually(t, func() bool { return attempts.Load() >= 3 },
		20*time.Second, 50*time.Millisecond,
		"handler must be invoked 3 times — 2 failures + 1 success")

	// Give the consumer a beat to ack and try fetching the next
	// (non-existent) message — attempts must NOT exceed 3.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int32(3), attempts.Load(),
		"no further retry after success")
}

// -----------------------------------------------------------------------------
// 4. Closed publisher contract.
// -----------------------------------------------------------------------------

func TestRabbitMQPublisher_ClosedPublisherRejectsPublish(t *testing.T) {
	require.NotEmpty(t, env.RabbitURL())

	pub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	require.NoError(t, pub.Close())
	require.NoError(t, pub.Close(), "Close must be idempotent")

	err = pub.Publish(t.Context(), newEnvelope("evt-after-close", "w_x", "USDC", 1, time.Now().UTC()))
	require.Error(t, err, "Publish on a closed publisher must return an error")
}

