//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	brokerrabbit "github.com/brunocampos-ssa/portfolio-api/internal/broker/rabbitmq"
	"github.com/brunocampos-ssa/portfolio-api/internal/notifier"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/router"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// event-notifier integration tests
// =============================================================================
//
// Two scenarios:
//
//   1. Notifier in isolation — publish directly to the RabbitMQ topic
//      exchange, run the notifier with a narrow binding pattern,
//      assert it only sees envelopes matching that pattern. Locks the
//      contract that the notify callback fires for the right
//      subset of the firehose.
//
//   2. Full Class 2 pipeline — the chapter's headline test:
//      blockchain → poller → Kafka → router → RabbitMQ → notifier.
//      Fires a real USDC transfer on the forked Anvil and asserts the
//      notifier's callback eventually sees it. Five separate
//      components, two transports, one event identity making it
//      through every hop.

// -----------------------------------------------------------------------------
// 1. Notifier in isolation
// -----------------------------------------------------------------------------

func TestEventNotifier_FiresOnlyOnMatchingRoutingKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.RabbitURL())

	// Publisher used by the test to inject events directly into the
	// exchange — simulating what event-router would emit.
	pub, err := brokerrabbit.NewPublisher(env.RabbitURL(), exchangeUnderTest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Notifier subscribes to "*.incoming.*" — only incoming transfers.
	queueName := uniqueQueue(t, "notifier-incoming")
	cons, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), exchangeUnderTest, queueName, "*.incoming.*",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cons.Close() })

	alerted := make(chan *broker.EventEnvelope, 8)
	notify := func(_ context.Context, e *broker.EventEnvelope) error {
		alerted <- e
		return nil
	}
	handler := notifier.NewHandler(notify)

	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = cons.Run(runCtx, handler)
		close(done)
	}()
	t.Cleanup(func() {
		cancelRun()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("notifier consumer did not exit within 5s")
		}
	})

	// Build envelopes with explicit (direction, token) so the routing
	// keys are predictable.
	mk := func(eventID, direction, token string) *broker.EventEnvelope {
		e := newEnvelope(eventID, "w_x", token, 1, time.Now().UTC())
		e.Direction = direction
		return e
	}
	all := []*broker.EventEnvelope{
		mk("ntf-1", "incoming", "USDC"), // matches binding
		mk("ntf-2", "outgoing", "USDC"), // does NOT match (outgoing)
		mk("ntf-3", "incoming", "USDT"), // matches binding
		mk("ntf-4", "outgoing", "ETH"),  // does NOT match
	}
	want := []*broker.EventEnvelope{all[0], all[2]}

	for _, e := range all {
		require.NoError(t, pub.Publish(ctx, e),
			"publish %s rk=%s", e.EventID, broker.RoutingKey(e))
	}

	got := make([]*broker.EventEnvelope, 0, len(want))
	for len(got) < len(want) {
		select {
		case e := <-alerted:
			got = append(got, e)
		case <-time.After(15 * time.Second):
			t.Fatalf("notifier missed events: got %d/%d", len(got), len(want))
		}
	}
	require.ElementsMatch(t, want, got,
		"notifier must alert only on envelopes whose routing key matches *.incoming.*")

	// Give the broker a beat — the outgoing events should NEVER reach
	// the notifier's queue. require.Empty on the channel confirms it.
	time.Sleep(500 * time.Millisecond)
	require.Empty(t, alerted, "outgoing events must not reach a *.incoming.* notifier")
}

// -----------------------------------------------------------------------------
// 2. The headline: full Class 2 pipeline, blockchain to notification
// -----------------------------------------------------------------------------

// TestModule4Class2_FullPipeline_BlockchainToNotification is the
// chapter's existence proof. Five components running in-process:
//
//   1. watcher (poller → normalize → kafka.Publisher)
//   2. kafka.Consumer in group "router"
//   3. router.NewHandler → rabbitmq.Publisher
//   4. rabbitmq.Consumer with binding "*.incoming.*"
//   5. notifier.NewHandler → capture callback
//
// We fire a real USDC transfer on the forked Anvil and assert the
// notifier callback eventually sees it. If any hop breaks the chain
// (wrong topic, wrong routing key, broken envelope conversion at
// either bus boundary, dropped at any consumer's retry exhaustion)
// the test times out. Two transports, one event identity, end to end.
//
// This is the test we'd open the chapter walkthrough with — it makes
// the whole topology tangible in one screen of code.
func TestModule4Class2_FullPipeline_BlockchainToNotification(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	require.NotEmpty(t, env.RabbitURL())

	// --- Pick a tracked wallet from the DB. ---
	trackedAddrs, err := env.GetTrackedETHAddresses(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, trackedAddrs)
	tracked := trackedAddrs[0]
	usdc := env.Fixtures.Tokens["USDC"]

	// --- Unique infrastructure names so this test doesn't see leftover
	//     state from other tests sharing the testenv brokers. ---
	kafkaTopic := fmt.Sprintf("wallet.events.test.full-pipeline-%d", time.Now().UnixNano())
	createTopic(t, env.KafkaBrokers(), kafkaTopic, 3)

	// We use a unique RABBITMQ EXCHANGE for this test so the notifier
	// queue's binding can't be polluted by the standard wallet.events
	// exchange's leftover bindings from other tests.
	rabbitExchange := fmt.Sprintf("wallet.events.test.full-pipeline-%d", time.Now().UnixNano())

	// --- (1) Watcher: poller → normalize → kafka.Publisher ---
	watcherPub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), kafkaTopic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = watcherPub.Close() })

	walletRepo := postgres.NewWalletRepository(env.DB)
	logsFetcher := blockchain.NewEthereumLogsFetcher(env.RPCURL())
	w := watcher.NewWatcher(logsFetcher, walletRepo, watcherPub, 500*time.Millisecond)

	wCtx, wCancel := context.WithCancel(ctx)
	wDone := make(chan struct{})
	go func() { _ = w.Run(wCtx); close(wDone) }()
	t.Cleanup(func() {
		wCancel()
		select {
		case <-wDone:
		case <-time.After(5 * time.Second):
			t.Errorf("watcher did not exit within 5s")
		}
	})

	// --- (2)+(3) Router: kafka.Consumer → router handler → rabbitmq.Publisher ---
	rabbitPub, err := brokerrabbit.NewPublisher(env.RabbitURL(), rabbitExchange)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rabbitPub.Close() })

	kafkaCons, err := brokerkafka.NewConsumer(
		env.KafkaBrokers(), kafkaTopic,
		fmt.Sprintf("wallet-events-router-test-fullpipe-%d", time.Now().UnixNano()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kafkaCons.Close() })

	rCtx, rCancel := context.WithCancel(ctx)
	rDone := make(chan struct{})
	go func() {
		_ = kafkaCons.Run(rCtx, router.NewHandler(rabbitPub))
		close(rDone)
	}()
	t.Cleanup(func() {
		rCancel()
		select {
		case <-rDone:
		case <-time.After(5 * time.Second):
			t.Errorf("router did not exit within 5s")
		}
	})

	// --- (4)+(5) Notifier: rabbitmq.Consumer → notifier handler ---
	notifierQueue := uniqueQueue(t, "notifier-fullpipe")
	notifierCons, err := brokerrabbit.NewConsumer(
		env.RabbitURL(), rabbitExchange, notifierQueue, "*.incoming.*",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = notifierCons.Close() })

	alerted := make(chan *broker.EventEnvelope, 4)
	nCtx, nCancel := context.WithCancel(ctx)
	nDone := make(chan struct{})
	go func() {
		_ = notifierCons.Run(nCtx, notifier.NewHandler(func(_ context.Context, e *broker.EventEnvelope) error {
			alerted <- e
			return nil
		}))
		close(nDone)
	}()
	t.Cleanup(func() {
		nCancel()
		select {
		case <-nDone:
		case <-time.After(5 * time.Second):
			t.Errorf("notifier did not exit within 5s")
		}
	})

	// --- Trigger the on-chain event AFTER all five components are up.
	//     Otherwise the watcher's first poll catches up to "latest" and
	//     the new event slips in cleanly. ---
	txHash, err := env.TransferERC20(ctx,
		usdc.Contract,
		usdc.Whale,
		tracked,
		big.NewInt(500_000_000), // 500.0 USDC (6 decimals)
	)
	require.NoError(t, err)
	require.NotEmpty(t, txHash)

	// --- Assert the alert lands. Patient deadline because we're
	//     crossing five hops including network calls to Anvil and
	//     two broker round-trips. ---
	deadline := time.After(60 * time.Second)
	for {
		select {
		case e := <-alerted:
			if !strings.EqualFold(e.TxHash, txHash) {
				// Re-deliveries of stale events (deterministic
				// EventIDs make them harmless) — keep waiting.
				continue
			}
			require.Equal(t, "ethereum", e.Network)
			require.Equal(t, "incoming", e.Direction)
			require.Equal(t, "USDC", e.TokenSymbol)
			require.NotEmpty(t, e.WalletID)
			t.Logf("✓ end-to-end pipeline delivered event_id=%s tx=%s", e.EventID, e.TxHash)
			return
		case <-deadline:
			t.Fatalf("the full pipeline did not deliver the on-chain event within 60s")
		}
	}
}

