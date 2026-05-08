package kafka

import (
	"context"
	"errors"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Consumer implements broker.Consumer against a Kafka cluster using a
// consumer group.
//
// TODO Class 2: wrap a *kafka.Reader configured with:
//   - GroupID = the consumer-group identifier (each cmd/event-* uses a
//     distinct one — see broker/topics.go for the constants).
//   - StartOffset = FirstOffset on first run so the group catches up
//     on history; subsequent runs resume from committed offsets.
//   - CommitInterval = 0 forces synchronous commits — at-least-once
//     semantics, with idempotent handlers absorbing the duplicate-
//     delivery risk. We will explicitly NOT use auto-commit; commits
//     happen after the handler returns nil.
type Consumer struct {
	brokers []string
	topic   string
	groupID string

	// reader is the kafka-go group-aware consumer. nil in the skeleton;
	// Class 2 will populate it in NewConsumer.
	reader *kafkago.Reader
}

// NewConsumer builds a consumer bound to a specific group. Different
// services using the same topic with different group IDs each get an
// independent view of the stream — that's the headline Kafka teaching
// moment for Class 2.
func NewConsumer(brokers []string, topic, groupID string) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka.NewConsumer: brokers must not be empty")
	}
	if topic == "" {
		return nil, errors.New("kafka.NewConsumer: topic must not be empty")
	}
	if groupID == "" {
		return nil, errors.New("kafka.NewConsumer: groupID must not be empty")
	}
	return &Consumer{brokers: brokers, topic: topic, groupID: groupID}, nil
}

// Run consumes envelopes until ctx is cancelled. Handler errors trigger
// a retry by NOT committing the offset.
//
// TODO Class 2: loop on reader.FetchMessage, broker.Unmarshal the value,
// invoke handler, and on nil call reader.CommitMessages.
func (c *Consumer) Run(_ context.Context, _ broker.Handler) error {
	return errors.New("kafka.Consumer.Run: not implemented (Class 2 stub)")
}

// Close stops the reader and waits for in-flight fetches to drain.
func (c *Consumer) Close() error {
	return nil
}

var _ broker.Consumer = (*Consumer)(nil)
