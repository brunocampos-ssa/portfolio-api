//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	brokerrabbit "github.com/brunocampos-ssa/portfolio-api/internal/broker/rabbitmq"
	"github.com/brunocampos-ssa/portfolio-api/internal/router"
)

// =============================================================================
// event-router integration tests
// =============================================================================
//
// Two scenarios:
//
//   1. The headline bridge test: publish to Kafka → run the router →
//      consume from a RabbitMQ queue bound with "#". Asserts every
//      envelope arrives intact, with the EventID preserved end-to-end.
//
//   2. Routing-key derivation: publish events with a mix of
//      (direction × token) → bind two RabbitMQ queues with narrow
//      patterns → assert each receives only its slice. This proves
//      the router constructs routing keys correctly from the
//      envelope, not just that something gets through.
//
// Each test uses unique Kafka topic + RabbitMQ queue names so re-runs
// and parallel tests on the shared testenv brokers don't collide.

// -----------------------------------------------------------------------------
// 1. Bridge happy path
// -----------------------------------------------------------------------------

func TestEventRouter_BridgesKafkaToRabbitMQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	require.NotEmpty(t, env.RabbitURL())

	// --- Kafka source (input side of the bridge) ---
	kafkaTopic := fmt.Sprintf("wallet.events.test.router-bridge-%d", time.Now().UnixNano())
	createTopic(t, env.KafkaBrokers(), kafkaTopic, 3)

	kafkaPub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), kafkaTopic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kafkaPub.Close() })

	// --- RabbitMQ side (output side of the bridge) ---
	rabbitPub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rabbitPub.Close() })

	// Sink queue that accepts everything the router emits. Unique
	// name per test run — see uniqueQueue() in the rabbit test file.
	sinkQueue := uniqueQueue(t, "router-sink")
	sink, err := brokerrabbit.NewConsumer(env.RabbitURL(), exchangeUnderTest, sinkQueue, "#")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close() })

	received := make(chan *broker.EventEnvelope, 8)
	runConsumer(t, sink, ctx, func(_ context.Context, e *broker.EventEnvelope) error {
		received <- e
		return nil
	})

	// --- Router under test ---
	kafkaCons, err := brokerkafka.NewConsumer(
		env.KafkaBrokers(),
		kafkaTopic,
		fmt.Sprintf("wallet-events-router-test-bridge-%d", time.Now().UnixNano()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kafkaCons.Close() })

	handler := router.NewHandler(rabbitPub)
	routerCtx, cancelRouter := context.WithCancel(ctx)
	routerDone := make(chan struct{})
	go func() {
		_ = kafkaCons.Run(routerCtx, handler)
		close(routerDone)
	}()
	t.Cleanup(func() {
		cancelRouter()
		select {
		case <-routerDone:
		case <-time.After(5 * time.Second):
			t.Errorf("router did not exit within 5s of cancel")
		}
	})

	// --- Publish source envelopes ---
	want := []*broker.EventEnvelope{
		newEnvelope("evt-bridge-1", "w_a", "USDC", 100, time.Now().UTC()),
		newEnvelope("evt-bridge-2", "w_b", "USDT", 101, time.Now().UTC()),
		newEnvelope("evt-bridge-3", "w_a", "ETH", 102, time.Now().UTC()),
	}
	for _, e := range want {
		require.NoError(t, kafkaPub.Publish(ctx, e), "publish %s to kafka", e.EventID)
	}

	// --- Assert each envelope arrives on the RabbitMQ side ---
	got := make([]*broker.EventEnvelope, 0, len(want))
	for len(got) < len(want) {
		select {
		case e := <-received:
			got = append(got, e)
		case <-time.After(30 * time.Second):
			t.Fatalf("router did not bridge all envelopes: got %d/%d", len(got), len(want))
		}
	}
	require.ElementsMatch(t, want, got,
		"every envelope published to Kafka must arrive in the RabbitMQ sink queue")
}

// -----------------------------------------------------------------------------
// 2. Routing-key derivation
// -----------------------------------------------------------------------------

// TestEventRouter_DerivesRoutingKeyFromEnvelope proves the router
// constructs RabbitMQ routing keys from the envelope's
// (network, direction, token) — not from anything else (Kafka
// partition, ad-hoc constant, ...). We test it the only way that
// matters: bind two queues with narrow patterns and assert each gets
// exactly the slice its pattern matches.
//
// If the router published with a constant routing key (or just the
// envelope's WalletID, etc.), the narrow-bound queues would receive
// nothing. The test would time out.
func TestEventRouter_DerivesRoutingKeyFromEnvelope(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	require.NotEmpty(t, env.RabbitURL())

	kafkaTopic := fmt.Sprintf("wallet.events.test.router-rk-%d", time.Now().UnixNano())
	createTopic(t, env.KafkaBrokers(), kafkaTopic, 3)

	kafkaPub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), kafkaTopic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kafkaPub.Close() })

	rabbitPub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rabbitPub.Close() })

	// Two narrow-pattern sinks.
	incomingQ := uniqueQueue(t, "router-incoming")
	consIncoming, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), exchangeUnderTest, incomingQ, "*.incoming.*",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consIncoming.Close() })

	usdcQ := uniqueQueue(t, "router-usdc")
	consUSDC, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), exchangeUnderTest, usdcQ, "*.*.usdc",
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

	// --- Router under test ---
	kafkaCons, err := brokerkafka.NewConsumer(
		env.KafkaBrokers(),
		kafkaTopic,
		fmt.Sprintf("wallet-events-router-test-rk-%d", time.Now().UnixNano()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kafkaCons.Close() })

	routerCtx, cancelRouter := context.WithCancel(ctx)
	routerDone := make(chan struct{})
	go func() {
		_ = kafkaCons.Run(routerCtx, router.NewHandler(rabbitPub))
		close(routerDone)
	}()
	t.Cleanup(func() {
		cancelRouter()
		select {
		case <-routerDone:
		case <-time.After(5 * time.Second):
			t.Errorf("router did not exit within 5s of cancel")
		}
	})

	// Build envelopes with explicit (direction, token) so the
	// routing keys are predictable. Helper varies from newEnvelope()
	// which hard-codes incoming/USDC.
	mk := func(eventID, direction, token string) *broker.EventEnvelope {
		e := newEnvelope(eventID, "w_x", token, 1, time.Now().UTC())
		e.Direction = direction
		return e
	}
	all := []*broker.EventEnvelope{
		mk("rrk-1", "incoming", "USDC"), // matches BOTH narrow queues
		mk("rrk-2", "incoming", "USDT"), // matches incoming only
		mk("rrk-3", "outgoing", "USDC"), // matches usdc only
		mk("rrk-4", "outgoing", "ETH"),  // matches NEITHER
	}
	wantIncoming := []*broker.EventEnvelope{all[0], all[1]}
	wantUSDC := []*broker.EventEnvelope{all[0], all[2]}

	for _, e := range all {
		require.NoError(t, kafkaPub.Publish(ctx, e),
			"publish %s rk=%s", e.EventID, broker.RoutingKey(e))
	}

	collect := func(t *testing.T, ch <-chan *broker.EventEnvelope, n int, label string) []*broker.EventEnvelope {
		t.Helper()
		out := make([]*broker.EventEnvelope, 0, n)
		for len(out) < n {
			select {
			case e := <-ch:
				out = append(out, e)
			case <-time.After(30 * time.Second):
				t.Fatalf("%s: timeout, got %d/%d", label, len(out), n)
			}
		}
		return out
	}

	gotIncomingList := collect(t, gotIncoming, len(wantIncoming), "router → *.incoming.* queue")
	gotUSDCList := collect(t, gotUSDC, len(wantUSDC), "router → *.*.usdc queue")

	require.ElementsMatch(t, wantIncoming, gotIncomingList,
		"router must derive routing keys correctly so *.incoming.* receives both incoming events and only those")
	require.ElementsMatch(t, wantUSDC, gotUSDCList,
		"router must derive routing keys correctly so *.*.usdc receives both USDC events and only those")

	// Give the broker a beat — the outgoing.eth event matches
	// neither pattern, so neither queue should see more than its
	// expected count.
	time.Sleep(500 * time.Millisecond)
	require.Empty(t, gotIncoming, "no extra messages")
	require.Empty(t, gotUSDC, "no extra messages")
}
