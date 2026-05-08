// Package kafka is the segmentio/kafka-go adapter for broker.Publisher
// and broker.Consumer.
//
// We picked kafka-go (over confluent-kafka-go and IBM/sarama) because it
// is pure Go (no CGO, no librdkafka), the source is small enough for a
// student to read end-to-end in an afternoon, and the API maps almost
// 1:1 to the concepts the chapter teaches: Reader = consumer with an
// internal offset tracker, Writer = producer with batching and acks.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Publisher implements broker.Publisher against a Kafka cluster.
//
// The configuration choices below are deliberate teaching points:
//
//   - RequiredAcks = RequireAll: the broker waits for every in-sync
//     replica to write the message before acking. Durability over
//     latency — the right default for an event log of record. With
//     a single-broker dev cluster this is equivalent to RequireOne;
//     in production with replication factor ≥ 3 it gives you the
//     "no data loss on broker failure" property.
//
//   - Balancer = Hash: partitions are chosen by hashing the message
//     key. We key on envelope.WalletID, so events for the same wallet
//     always land on the same partition and stay strictly ordered.
//     The cost: a hot wallet skews load to one partition. For our
//     workload that's fine; alternatives (CRC32, Murmur2) trade
//     determinism for distribution.
//
//   - Compression = Snappy: cheap CPU-wise, supported by every
//     kafka client, ~3-5x payload reduction on JSON envelopes. We
//     do NOT use lz4/zstd here — both have wider client support
//     gaps and Snappy is the long-time community default.
//
//   - Async = false: WriteMessages blocks until the broker acks.
//     This is the lever that lets the watcher's pipeline apply
//     backpressure when Kafka is slow or down.
//
//   - WriteTimeout / ReadTimeout: 10s. Kafka network latencies
//     >10s usually mean a real outage rather than transient
//     slowness; failing fast lets the watcher log and retry on
//     the next poll cycle rather than block its entire pipeline.
type Publisher struct {
	// brokers is the bootstrap server list.
	brokers []string

	// topic is the destination. Set once at construction; Class 2 only
	// uses one topic, broker.KafkaTopicWalletEvents.
	topic string

	// writer is the kafka-go batching producer. Created in NewPublisher
	// and re-used across all Publish calls — kafka-go writers are safe
	// for concurrent use from multiple goroutines.
	writer *kafkago.Writer
}

// NewPublisher constructs a configured Kafka publisher. brokers must be
// non-empty; topic must be non-empty. Returns an error rather than
// panicking so wire-up code in cmd/* can log and exit cleanly.
func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka.NewPublisher: brokers must not be empty")
	}
	if topic == "" {
		return nil, errors.New("kafka.NewPublisher: topic must not be empty")
	}

	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Compression:            kafkago.Snappy,
		Async:                  false,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		AllowAutoTopicCreation: true, // dev convenience; prod sets ACLs that forbid it
	}

	return &Publisher{
		brokers: brokers,
		topic:   topic,
		writer: w,
	}, nil
}

// Publish writes one envelope to Kafka. Blocks until the broker acks
// per the writer's RequiredAcks setting (or ctx is cancelled).
//
// Partition key is the envelope's WalletID — events for the same
// wallet are guaranteed to land on the same partition and therefore
// preserve their on-chain ordering on the consumer side. Cross-wallet
// ordering is NOT preserved across partitions; consumers that need a
// global timeline can sort by (block_number, event_id) post-read.
func (p *Publisher) Publish(ctx context.Context, env *broker.EventEnvelope) error {
	if p.writer == nil {
		return errors.New("kafka.Publisher.Publish: publisher is closed")
	}
	if env == nil {
		return errors.New("kafka.Publisher.Publish: envelope must not be nil")
	}

	value, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("kafka.Publisher.Publish: marshal: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(env.WalletID),
		Value: value,
		Time:  env.EmittedAt,
		Headers: []kafkago.Header{
			// Mirror the schema version into a header so consumers can
			// reject incompatible messages without parsing the body.
			// Useful for DLQ routing in a future class.
			{Key: "schema_version", Value: fmt.Appendf(nil, "%d", env.SchemaVersion)},
			{Key: "event_id", Value: []byte(env.EventID)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka.Publisher.Publish: write: %w", err)
	}
	return nil
}

// Close flushes any in-flight batches and closes the underlying TCP
// connections. After Close, further Publish calls return an error
// (the writer is set to nil so the closed-publisher check trips).
//
// Idempotent: calling Close twice is safe.
func (p *Publisher) Close() error {
	if p.writer == nil {
		return nil
	}
	w := p.writer
	p.writer = nil
	if err := w.Close(); err != nil {
		return fmt.Errorf("kafka.Publisher.Close: %w", err)
	}
	return nil
}

// Compile-time check that *Publisher satisfies the broker.Publisher
// contract. Catches signature drift the moment it happens, not at the
// call site three packages away.
var _ broker.Publisher = (*Publisher)(nil)
