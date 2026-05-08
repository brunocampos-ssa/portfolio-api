//go:build integration

package integration

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	"github.com/brunocampos-ssa/portfolio-api/internal/persister"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// event-persister full-path integration tests
// =============================================================================
//
// Two scenarios:
//
//   1. Watcher → Kafka → Persister → DB. The headline test that
//      proves the chapter's full path: a real USDC transfer on the
//      forked Anvil triggers a watcher publish, which the persister
//      consumes and persists. Asserts the row in wallet_events
//      matches the on-chain reality.
//
//   2. Idempotency under at-least-once. We publish the same envelope
//      twice (deterministic EventID, just like a Kafka redelivery
//      would do) and assert exactly one row lands in the DB. This
//      validates the ON CONFLICT (id) DO NOTHING contract that the
//      persister relies on for safety.
//
// Both tests share the unified testenv (Postgres + forked Anvil +
// Kafka). Each uses a unique topic so they don't see each other's
// leftover messages.

// -----------------------------------------------------------------------------
// 1. Full path
// -----------------------------------------------------------------------------

func TestEventPersister_FullPath_BlockchainToDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	trackedAddrs, err := env.GetTrackedETHAddresses(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, trackedAddrs)
	tracked := trackedAddrs[0]

	usdc := env.Fixtures.Tokens["USDC"]

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.persister-fullpath"
	createTopic(t, env.KafkaBrokers(), topic, 3)

	// --- Watcher ------------------------------------------------------------
	watcherPub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = watcherPub.Close() })

	walletRepo := postgres.NewWalletRepository(env.DB)
	logsFetcher := blockchain.NewEthereumLogsFetcher(env.RPCURL())
	w := watcher.NewWatcher(logsFetcher, walletRepo, watcherPub, 500*time.Millisecond)

	wCtx, wCancel := context.WithCancel(ctx)
	wDone := make(chan error, 1)
	go func() { wDone <- w.Run(wCtx) }()
	t.Cleanup(func() {
		wCancel()
		select {
		case e := <-wDone:
			if e != nil && !errors.Is(e, context.Canceled) {
				t.Logf("watcher exit: %v", e)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("watcher did not exit within 5s of cancel")
		}
	})

	// --- Persister ----------------------------------------------------------
	eventRepo := postgres.NewEventRepository(env.DB)
	handler := persister.NewHandler(eventRepo)

	// Use a unique consumer-group ID so this test's offsets don't
	// collide with the idempotency test below (or the production
	// group name).
	groupID := "wallet-events-persister-test-fullpath"
	consumer, err := brokerkafka.NewConsumer(env.KafkaBrokers(), topic, groupID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	pCtx, pCancel := context.WithCancel(ctx)
	pDone := make(chan error, 1)
	go func() { pDone <- consumer.Run(pCtx, handler) }()
	t.Cleanup(func() {
		pCancel()
		select {
		case e := <-pDone:
			if e != nil && !errors.Is(e, context.Canceled) {
				t.Logf("persister exit: %v", e)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("persister did not exit within 5s of cancel")
		}
	})

	// --- Trigger an on-chain event ----------------------------------------
	txHash, err := env.TransferERC20(ctx,
		usdc.Contract,
		usdc.Whale,
		tracked,
		big.NewInt(500_000_000),
	)
	require.NoError(t, err)
	require.NotEmpty(t, txHash)

	// --- Wait for the row to land ----------------------------------------
	require.Eventually(t, func() bool {
		var cnt int
		if err := env.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM wallet_events
			WHERE tx_hash = $1 AND direction = 'incoming'
		`, txHash).Scan(&cnt); err != nil {
			return false
		}
		return cnt > 0
	}, 30*time.Second, 250*time.Millisecond,
		"persister must consume from Kafka and land the event in wallet_events")

	// --- Sanity: the row is shaped correctly --------------------------------
	var (
		gotID, gotWallet, gotToken, gotDir, gotEventType string
		gotBlock                                         uint64
	)
	require.NoError(t, env.DB.QueryRowContext(ctx, `
		SELECT id, wallet_id, token_symbol, direction, event_type, block_number
		FROM wallet_events WHERE tx_hash = $1 LIMIT 1`,
		txHash,
	).Scan(&gotID, &gotWallet, &gotToken, &gotDir, &gotEventType, &gotBlock))
	require.True(t, strings.HasPrefix(gotID, "evt_"),
		"row id must be the deterministic envelope EventID, got %q", gotID)
	require.NotEmpty(t, gotWallet)
	require.Equal(t, "USDC", gotToken)
	require.Equal(t, "incoming", gotDir)
	require.Equal(t, "transfer", gotEventType)
	require.Greater(t, gotBlock, uint64(0))
}

// -----------------------------------------------------------------------------
// 2. Idempotency under at-least-once redelivery
// -----------------------------------------------------------------------------

// TestEventPersister_DuplicateDeliveryYieldsOneRow proves the
// at-least-once-safe property the architecture leans on: publishing
// the SAME envelope (same EventID — exactly what a Kafka redelivery
// produces) twice results in exactly one row in wallet_events.
//
// We can't easily simulate Kafka redelivery directly, so we publish
// the same envelope twice: same effect at the persister's input.
// ON CONFLICT (id) DO NOTHING is the load-bearing piece.
func TestEventPersister_DuplicateDeliveryYieldsOneRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers())
	topic := "wallet.events.test.persister-idempotent"
	createTopic(t, env.KafkaBrokers(), topic, 1)

	// Use a real, existing wallet from the seed data so the FK to
	// wallets(id) holds.
	row := env.DB.QueryRowContext(ctx,
		`SELECT id FROM wallets WHERE blockchain = 'ethereum' LIMIT 1`)
	var walletID string
	require.NoError(t, row.Scan(&walletID))

	// Same EventID + same TxHash → indistinguishable from a Kafka
	// redelivery to the persister.
	const eventID = "evt_idempotent_fixture"
	now := time.Now().UTC().Truncate(time.Millisecond)
	envlp := &broker.EventEnvelope{
		EventID:         eventID,
		SchemaVersion:   broker.SchemaCurrent,
		Network:         "ethereum",
		EventType:       "transfer",
		Direction:       "incoming",
		WalletID:        walletID,
		TokenSymbol:     "USDC",
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          "100.0",
		TxHash:          "0xidempotent",
		BlockNumber:     1,
		EmittedAt:       now,
	}

	// Publish twice — same envelope, same id.
	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	require.NoError(t, pub.Publish(ctx, envlp))
	require.NoError(t, pub.Publish(ctx, envlp))
	require.NoError(t, pub.Close())

	// Run the persister until both deliveries are consumed.
	eventRepo := postgres.NewEventRepository(env.DB)
	handler := persister.NewHandler(eventRepo)

	consumer, err := brokerkafka.NewConsumer(
		env.KafkaBrokers(), topic,
		"wallet-events-persister-test-idempotent",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(cCtx, handler)
	}()

	// Wait for the row count for this event_id to be 1 and stay 1.
	require.Eventually(t, func() bool {
		var cnt int
		if err := env.DB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM wallet_events WHERE id = $1`, eventID,
		).Scan(&cnt); err != nil {
			return false
		}
		return cnt == 1
	}, 20*time.Second, 200*time.Millisecond,
		"persister must land exactly one row for the duplicated envelope")

	// Give the consumer a beat to ack the second delivery and try
	// (and silently no-op) to insert again.
	time.Sleep(500 * time.Millisecond)

	var finalCount int
	require.NoError(t, env.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM wallet_events WHERE id = $1`, eventID,
	).Scan(&finalCount))
	require.Equal(t, 1, finalCount,
		"duplicate Kafka delivery must NOT produce a second wallet_events row")

	cCancel()
	wg.Wait()
}
