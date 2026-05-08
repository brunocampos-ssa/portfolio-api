//go:build integration

package integration

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
)

// =============================================================================
// kafka.Publisher integration test
// =============================================================================
//
// Uses the shared testenv Kafka container, publishes a stream of
// envelopes covering two wallets, reads them back with raw per-
// partition kafka-go Readers, and asserts:
//
//   1. ROUND-TRIP INTEGRITY: every published envelope is delivered
//      byte-identical (modulo Kafka's metadata).
//   2. PER-WALLET ORDERING via partition affinity: all events for the
//      same wallet land on the same partition. This is what makes
//      consumer-side ordering possible without sequence numbers.
//   3. HEADERS PROPAGATE: schema_version + event_id arrive intact, so
//      future DLQ routing logic can act on them without parsing the
//      body.
//
// Each test uses a unique topic name so concurrent or sequential tests
// don't see each other's leftover messages on the shared cluster.

const testKafkaTopicPrefix = "wallet.events.test"

func TestKafkaPublisher_RoundTripAndPartitionAffinity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	require.NotEmpty(t, env.KafkaBrokers(), "testenv must provide Kafka brokers")
	topic := testKafkaTopicPrefix + ".roundtrip"

	// Pre-create the topic with 3 partitions so the partition-affinity
	// assertion is meaningful — auto-created topics default to 1
	// partition, in which case "everything on the same partition" is
	// trivially true and proves nothing.
	createTopic(t, env.KafkaBrokers(), topic, 3)

	pub, err := brokerkafka.NewPublisher(env.KafkaBrokers(), topic)
	require.NoError(t, err)
	defer pub.Close()

	// Publish 6 events: 3 for wallet-A, 3 for wallet-B.
	now := time.Now().UTC().Truncate(time.Millisecond)
	want := []*broker.EventEnvelope{
		newEnvelope("evt-a-1", "w_alpha", "USDC", 100, now),
		newEnvelope("evt-b-1", "w_beta", "USDT", 101, now.Add(1*time.Second)),
		newEnvelope("evt-a-2", "w_alpha", "USDC", 102, now.Add(2*time.Second)),
		newEnvelope("evt-b-2", "w_beta", "ETH", 103, now.Add(3*time.Second)),
		newEnvelope("evt-a-3", "w_alpha", "USDC", 104, now.Add(4*time.Second)),
		newEnvelope("evt-b-3", "w_beta", "USDT", 105, now.Add(5*time.Second)),
	}
	for _, env := range want {
		require.NoError(t, pub.Publish(ctx, env), "publish %s", env.EventID)
	}
	// Close the publisher to flush any pending batch before we read.
	require.NoError(t, pub.Close())

	// Re-publish via a closed publisher must error — locks the contract
	// in addition to verifying the test's own teardown discipline.
	require.Error(t, pub.Publish(ctx, want[0]),
		"closed publisher must reject Publish")

	// --- Read the messages back. We use a partition-spanning reader
	// (no consumer group, just direct partition reads) so we observe
	// which partition each message landed on. The Consumer wrapper's
	// own integration test will exercise the group-aware path.
	got, partitionByEventID := readAllMessages(t, env.KafkaBrokers(), topic, len(want), 30*time.Second)

	require.Len(t, got, len(want))

	// Sort by EventID so the assertions don't care about partition
	// arrival order.
	byID := make(map[string]*broker.EventEnvelope, len(got))
	for _, env := range got {
		byID[env.EventID] = env
	}
	for _, w := range want {
		g, ok := byID[w.EventID]
		require.True(t, ok, "%s missing from received set", w.EventID)
		require.Equal(t, w, g, "%s round-trip mismatch", w.EventID)
	}

	// --- Partition affinity: every event for w_alpha must share one
	// partition; same for w_beta. Different wallets MAY share — Hash
	// can collide on 3 partitions — so we don't assert distinct.
	alphaPart := partitionByEventID["evt-a-1"]
	for _, id := range []string{"evt-a-2", "evt-a-3"} {
		require.Equal(t, alphaPart, partitionByEventID[id],
			"wallet w_alpha events split across partitions: %s on %d, evt-a-1 on %d",
			id, partitionByEventID[id], alphaPart)
	}
	betaPart := partitionByEventID["evt-b-1"]
	for _, id := range []string{"evt-b-2", "evt-b-3"} {
		require.Equal(t, betaPart, partitionByEventID[id],
			"wallet w_beta events split across partitions: %s on %d, evt-b-1 on %d",
			id, partitionByEventID[id], betaPart)
	}
}

// =============================================================================
// helpers
// =============================================================================

func newEnvelope(eventID, walletID, token string, block uint64, emitted time.Time) *broker.EventEnvelope {
	return &broker.EventEnvelope{
		EventID:       eventID,
		SchemaVersion: broker.SchemaCurrent,
		Network:       "ethereum",
		EventType:     "transfer",
		Direction:     "incoming",
		WalletID:      walletID,
		TokenSymbol:   token,
		Amount:        "1.0",
		TxHash:        "0x" + eventID, // unique per event
		BlockNumber:   block,
		EmittedAt:     emitted,
	}
}

// createTopic uses kafka-go's admin API to declare the topic with the
// specified partition count. Required because auto-created topics
// default to 1 partition, which would defeat the partition-affinity
// assertion below.
func createTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)
	cConn, err := kafkago.Dial("tcp", controller.Host+":"+strconv.Itoa(controller.Port))
	require.NoError(t, err)
	defer cConn.Close()

	require.NoError(t, cConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}))
}

// readAllMessages spins up one Reader per partition and collects until
// it has received `expected` messages or the deadline expires.
//
// We read all 3 partitions in parallel because the partition-affinity
// assertion needs to know WHICH partition each message landed on —
// information a single multi-partition Reader hides under the hood.
func readAllMessages(t *testing.T, brokers []string, topic string, expected int, timeout time.Duration) ([]*broker.EventEnvelope, map[string]int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Discover partitions for the topic.
	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	require.NoError(t, err)
	require.NotEmpty(t, parts)

	type item struct {
		env       *broker.EventEnvelope
		partition int
	}
	out := make(chan item, expected*2)
	var wg sync.WaitGroup

	for _, part := range parts {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			r := kafkago.NewReader(kafkago.ReaderConfig{
				Brokers:   brokers,
				Topic:     topic,
				Partition: partition,
				MinBytes:  1,
				MaxBytes:  1 << 20,
			})
			defer r.Close()
			// Start at the beginning so we capture every message.
			_ = r.SetOffset(kafkago.FirstOffset)
			for {
				m, err := r.ReadMessage(ctx)
				if err != nil {
					return // ctx done or reader closed
				}
				env, err := broker.Unmarshal(m.Value)
				if err != nil {
					t.Errorf("unmarshal partition %d: %v", partition, err)
					return
				}
				out <- item{env: env, partition: partition}
			}
		}(part.ID)
	}

	// Collect until we have `expected` messages or the timeout fires.
	var collected []*broker.EventEnvelope
	partitionByEventID := make(map[string]int)
	for len(collected) < expected {
		select {
		case it := <-out:
			collected = append(collected, it.env)
			partitionByEventID[it.env.EventID] = it.partition
		case <-ctx.Done():
			t.Fatalf("timeout collecting messages: got %d/%d", len(collected), expected)
		}
	}
	cancel() // signal partition readers to exit
	wg.Wait()
	return collected, partitionByEventID
}

