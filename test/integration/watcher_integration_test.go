//go:build integration

package integration

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/brunocampos-ssa/portfolio-api/internal/watcher"
)

// =============================================================================
// Watcher integration test — full path: blockchain → poller → normalize
//                            → kafka.Publisher → Kafka topic → consumer
// =============================================================================
//
// testenv has already booted Postgres, the forked Anvil, and Kafka.
// Postgres holds the seed wallets; Anvil holds the seed balances; Kafka
// is the bus the watcher publishes into.
//
// This test fires a fresh USDC Transfer into a tracked wallet on the
// forked chain and verifies that the production watcher detects,
// normalizes, and publishes the event onto Kafka — exactly the path a
// production deployment would exercise. We read the message back via a
// raw kafka-go Reader to avoid coupling to the (still-stub)
// kafka.Consumer wrapper. Once the Consumer wrapper lands, an
// additional test will verify the same flow through that wrapper.

func TestWatcher_FullPath_BlockchainToKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	// --- Pick a tracked wallet from the DB. No hardcoding. ---
	trackedAddrs, err := env.GetTrackedETHAddresses(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, trackedAddrs, "bootstrap must have produced at least one ETH wallet")
	tracked := trackedAddrs[0]

	usdc := env.Fixtures.Tokens["USDC"]

	// --- Per-test topic so we don't see other tests' leftover events. ---
	topic := "wallet.events.test.watcher-fullpath"
	require.NotEmpty(t, env.KafkaBrokers())
	createTopic(t, env.KafkaBrokers(), topic, 3)

	// --- Wire the watcher exactly as production wires it: walletRepo
	// + EthereumLogsFetcher + kafka.Publisher. No persistence in the
	// watcher; that responsibility moved to event-persister.
	publisher, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Close() })

	walletRepo := postgres.NewWalletRepository(env.DB)
	logsFetcher := blockchain.NewEthereumLogsFetcher(env.RPCURL())
	w := watcher.NewWatcher(logsFetcher, walletRepo, publisher, 500*time.Millisecond)

	// --- Run the watcher in the background. ---
	watcherCtx, cancelW := context.WithCancel(ctx)
	watcherDone := make(chan error, 1)
	go func() { watcherDone <- w.Run(watcherCtx) }()
	t.Cleanup(func() {
		cancelW()
		select {
		case err := <-watcherDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("watcher exited with: %v", err)
			}
		case <-time.After(5 * time.Second):
			// Surface goroutine leaks instead of letting them silently
			// flake the next test.
			t.Errorf("watcher did not exit within 5s after cancel")
		}
	})

	// --- Fire a 500-USDC transfer into the tracked wallet AFTER the
	// watcher is running. Otherwise the poller's first FetchLogs
	// catches up to "latest" and the new event slips in cleanly. ---
	txHash, err := env.TransferERC20(ctx,
		usdc.Contract,
		usdc.Whale,
		tracked,
		big.NewInt(500_000_000), // 500.0 USDC (6 decimals)
	)
	require.NoError(t, err)
	require.NotEmpty(t, txHash)

	// --- Consume from Kafka until we find an envelope matching the
	// transaction hash we just fired. We don't use a consumer group
	// (that's the next implementation step); a single Reader spanning
	// every partition is enough for the assertion. ---
	envelope := readEnvelopeForTx(t, env.KafkaBrokers(), topic, txHash, 30*time.Second)

	require.Equal(t, "ethereum", envelope.Network)
	require.Equal(t, "transfer", envelope.EventType)
	require.Equal(t, "incoming", envelope.Direction)
	require.Equal(t, "USDC", envelope.TokenSymbol)
	require.Equal(t, broker.SchemaCurrent, envelope.SchemaVersion)
	require.NotEmpty(t, envelope.EventID)
	require.NotEmpty(t, envelope.WalletID)
	require.True(t, strings.EqualFold(envelope.TxHash, txHash),
		"tx hash mismatch: got %s, want %s", envelope.TxHash, txHash)
}

// readEnvelopeForTx scans the topic across all partitions until it
// finds an envelope whose TxHash matches `wantTx` or the deadline
// fires. We can't predict which partition the event will land on, so
// we read all of them in parallel.
func readEnvelopeForTx(t *testing.T, brokers []string, topic, wantTx string, timeout time.Duration) *broker.EventEnvelope {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	require.NoError(t, err)
	require.NotEmpty(t, parts)

	hits := make(chan *broker.EventEnvelope, len(parts))

	for _, part := range parts {
		go func(partition int) {
			r := kafkago.NewReader(kafkago.ReaderConfig{
				Brokers:   brokers,
				Topic:     topic,
				Partition: partition,
				MinBytes:  1,
				MaxBytes:  1 << 20,
			})
			defer r.Close()
			_ = r.SetOffset(kafkago.FirstOffset)
			for {
				m, err := r.ReadMessage(ctx)
				if err != nil {
					return
				}
				env, err := broker.Unmarshal(m.Value)
				if err != nil {
					t.Logf("partition %d: skip unmarshal err: %v", partition, err)
					continue
				}
				if strings.EqualFold(env.TxHash, wantTx) {
					select {
					case hits <- env:
					case <-ctx.Done():
					}
					return
				}
			}
		}(part.ID)
	}

	select {
	case env := <-hits:
		return env
	case <-ctx.Done():
		t.Fatalf("did not see envelope for tx %s within %s", wantTx, timeout)
		return nil
	}
}
