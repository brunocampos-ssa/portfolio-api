// Package kafka is the segmentio/kafka-go adapter for broker.Publisher
// and broker.Consumer.
//
// We picked kafka-go (over confluent-kafka-go and IBM/sarama) because it
// is pure Go (no CGO, no librdkafka), the source is small enough for a
// student to read end-to-end in an afternoon, and the API maps almost
// 1:1 to the concepts the chapter teaches: Reader = consumer with an
// internal offset tracker, Writer = producer with batching and acks.
//
// Class 2 will fill these stubs in. The shape is fixed now so the
// downstream cmd/event-* binaries can wire against it.
package kafka

import (
	"context"
	"errors"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Publisher implements broker.Publisher against a Kafka cluster.
//
// TODO Class 2: wrap a *kafka.Writer with:
//   - RequiredAcks = RequireAll (waits for ISR ack — durability over
//     latency, the right default for an event log).
//   - Balancer = &kafka.Hash{} keyed on wallet address so per-wallet
//     ordering is preserved across partitions.
//   - Compression = Snappy (cheap, well-supported by every kafka client).
//   - Async = false (we want backpressure on Publish, see broker.Publisher).
type Publisher struct {
	// brokers is the bootstrap server list. Kept as []string rather
	// than a single comma-joined string so callers can validate each
	// entry independently (DNS resolution, TLS config, ...).
	brokers []string

	// topic is the destination. Set once at construction; Class 2 only
	// uses one topic, broker.KafkaTopicWalletEvents.
	topic string

	// writer is the kafka-go batching producer. nil in the skeleton;
	// Class 2 will populate it inside NewPublisher.
	writer *kafkago.Writer
}

// NewPublisher constructs a Kafka publisher. brokers must be non-empty;
// topic must be non-empty.
//
// Class 2 will add config knobs (TLS, SASL, batch size). The skeleton
// keeps the signature minimal so a real wire-up only adds, never breaks.
func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka.NewPublisher: brokers must not be empty")
	}
	if topic == "" {
		return nil, errors.New("kafka.NewPublisher: topic must not be empty")
	}
	return &Publisher{brokers: brokers, topic: topic}, nil
}

// Publish writes one envelope to Kafka. Blocks until the broker acks
// (or ctx is cancelled).
//
// TODO Class 2: marshal envelope → kafka.Message{Key: walletAddress,
// Value: bytes, Time: env.EmittedAt}; call writer.WriteMessages(ctx, msg).
func (p *Publisher) Publish(_ context.Context, _ *broker.EventEnvelope) error {
	return errors.New("kafka.Publisher.Publish: not implemented (Class 2 stub)")
}

// Close flushes pending writes and closes the underlying connection.
//
// TODO Class 2: writer.Close() — kafka-go flushes on close.
func (p *Publisher) Close() error {
	return nil
}

// Compile-time check that *Publisher satisfies the broker.Publisher
// contract. Catches signature drift the moment it happens, not at the
// call site three packages away.
var _ broker.Publisher = (*Publisher)(nil)
