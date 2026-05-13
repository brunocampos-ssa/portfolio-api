package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// =============================================================================
// Consumer — group-aware Kafka reader with bounded retry
// =============================================================================
//
// The consumer's contract:
//
//   - FetchMessage advances a local cursor; CommitMessages records the
//     offset on the broker. They are deliberately separate so we can
//     run the handler BEFORE acknowledging — at-least-once semantics.
//
//   - On handler success: commit the offset.
//
//   - On handler error: retry the SAME message with exponential
//     backoff up to maxRetries. After exhaustion, commit anyway and
//     log loudly. A production system would route to a DLQ; we keep
//     the in-process model for Class 2 and let the next chapter add
//     a real DLQ topic.
//
//   - On schema-decode error: commit and skip. Otherwise a single
//     poison-pill from a future producer would freeze the partition
//     for every consumer in this group forever.
//
//   - On ctx cancellation: return ctx.Err(), preserving the offset
//     state. The next process start resumes from the last successful
//     commit, redelivering anything that was in flight.
//
// Class 2 lesson: idempotency is the HANDLER's responsibility, not
// the consumer's. The persister anchors on the deterministic EventID
// (wallet_events PRIMARY KEY + ON CONFLICT (id) DO NOTHING). The
// RabbitMQ publisher carries the same EventID through MessageId and
// an event_id header so notifiers can dedupe by event identity. The
// analytics consumer doesn't commit at all (it always replays — see
// event-analytics for that pattern).

const (
	// maxRetries bounds same-message retry attempts before giving up
	// and committing the offset. 3 is enough to absorb a transient
	// network blip (DB reconnect, downstream restart) without holding
	// up the partition indefinitely on a genuinely poisonous payload.
	maxRetries = 3

	// initialBackoff is the first inter-retry delay. Doubles each
	// attempt. With maxRetries=3 and initialBackoff=200ms, the worst
	// case spent on one bad message is ~200ms + 400ms = 600ms before
	// the third (and final) attempt.
	initialBackoff = 200 * time.Millisecond
)

// Consumer implements broker.Consumer against a Kafka cluster using a
// consumer group. Different services using the same topic with
// different group IDs each get an independent view of the stream —
// that's the headline Kafka teaching moment for Class 2.
//
// Configuration choices, all teaching points:
//
//   - StartOffset = FirstOffset: applied only on the FIRST run of a
//     fresh group. Subsequent process starts resume from committed
//     offsets. The persister catches up on history; the analytics
//     consumer is built via NewReplayConsumer, which gives it a
//     unique-per-process group ID so it's ALWAYS a fresh group and
//     therefore always replays from the beginning.
//
//   - CommitInterval = 0: kafka-go's auto-commit is disabled. Commits
//     happen explicitly via CommitMessages after the handler returns
//     nil. This is what makes "handler runs before ack" the contract.
//
//   - MinBytes / MaxBytes: 1 byte minimum (low latency, the watcher
//     emits one event per block tick) up to 10 MiB per fetch (well
//     above the largest envelope we'd ever produce).
//
//   - MaxWait = 1s: cap how long a fetch blocks waiting for fresh
//     data before returning empty. Lets ctx cancellation propagate
//     within a second even when the topic is idle.
type Consumer struct {
	brokers []string
	topic   string
	groupID string

	// replay disables CommitMessages. Set by NewReplayConsumer: the
	// analytics consumer doesn't track offsets because its group ID
	// is unique per process and nobody will ever resume from them.
	// Committing would be wasted broker writes that suggest a
	// resumable model the binary doesn't intend to offer.
	replay bool

	reader *kafkago.Reader
}

// NewConsumer builds a consumer bound to a specific group. Different
// services using the same topic with different group IDs each get an
// independent view of the stream.
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

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafkago.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MiB
		MaxWait:        time.Second,
		CommitInterval: 0, // synchronous commits via CommitMessages()
	})

	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		reader:  r,
	}, nil
}

// NewReplayConsumer builds a consumer that ALWAYS replays the topic
// from the earliest available offset on every process start. It's the
// pattern the analytics binary needs: each restart recomputes its
// running totals from t=0, so persistent offsets would be actively
// wrong.
//
// Two mechanisms make that work:
//
//  1. The group ID is unique-per-process: baseGroupID + "-" + nanos.
//     Kafka treats it as a brand-new group, so StartOffset=FirstOffset
//     kicks in. This is the same trick a kubectl-restarted pod uses
//     to "rewind" itself — no manual seek required.
//
//  2. CommitMessages is skipped (replay=true). Committing would write
//     offsets nobody will ever read (the group ID won't exist next
//     restart), which is wasteful and misleading in broker dashboards.
//
// The base group ID is exposed in logs and metrics so an operator can
// still distinguish the analytics fleet from other consumer groups.
func NewReplayConsumer(brokers []string, topic, baseGroupID string) (*Consumer, error) {
	if baseGroupID == "" {
		return nil, errors.New("kafka.NewReplayConsumer: baseGroupID must not be empty")
	}
	uniqueGroupID := fmt.Sprintf("%s-%d", baseGroupID, time.Now().UnixNano())
	c, err := NewConsumer(brokers, topic, uniqueGroupID)
	if err != nil {
		return nil, err
	}
	c.replay = true
	return c, nil
}

// Run drives the consume loop until ctx is cancelled. Returns ctx.Err()
// on graceful shutdown, or a wrapped error if the underlying reader
// fails in a non-recoverable way.
//
// Concurrency: Run MUST be called from a single goroutine. The
// underlying kafka-go Reader is not safe for concurrent FetchMessage.
func (c *Consumer) Run(ctx context.Context, handler broker.Handler) error {
	if c.reader == nil {
		return errors.New("kafka.Consumer.Run: consumer is closed")
	}
	if handler == nil {
		return errors.New("kafka.Consumer.Run: handler must not be nil")
	}

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafka.Consumer.Run: fetch: %w", err)
		}

		env, err := broker.Unmarshal(msg.Value)
		if err != nil {
			// Poison pill — committing-and-skipping protects every
			// future consumer in this group from being stuck on the
			// same byte sequence.
			log.Printf("kafka.Consumer[%s]: skip undecodable message partition=%d offset=%d: %v",
				c.groupID, msg.Partition, msg.Offset, err)
			if commitErr := c.commitOrSkip(ctx, msg); commitErr != nil {
				return commitErr
			}
			continue
		}

		if err := c.invokeWithRetry(ctx, handler, env); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Bounded retry exhausted. We commit so the partition
			// keeps moving; a real DLQ would catch this. Log loudly
			// so an operator can see what dropped on the floor.
			log.Printf("kafka.Consumer[%s]: handler exhausted retries event_id=%s tx=%s: %v",
				c.groupID, env.EventID, env.TxHash, err)
		}

		if err := c.commitOrSkip(ctx, msg); err != nil {
			return err
		}
	}
}

// commitOrSkip is the one place that decides whether an offset
// advance is persisted to the broker. Non-replay consumers commit;
// replay consumers (analytics) do not — see NewReplayConsumer's
// docstring for the why. The in-memory reader cursor advances on
// the next FetchMessage regardless.
func (c *Consumer) commitOrSkip(ctx context.Context, msg kafkago.Message) error {
	if c.replay {
		return nil
	}
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("kafka.Consumer.Run: commit: %w", err)
	}
	return nil
}

// invokeWithRetry calls handler up to maxRetries times with
// exponential backoff between attempts. Returns the last error if
// every attempt failed; nil on the first success.
//
// Cancellation propagates through the backoff sleep so a shutdown
// in the middle of retry doesn't add up to (maxRetries-1)*backoff
// of latency.
func (c *Consumer) invokeWithRetry(ctx context.Context, h broker.Handler, env *broker.EventEnvelope) error {
	backoff := initialBackoff
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := h(ctx, env)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == maxRetries {
			break
		}
		log.Printf("kafka.Consumer[%s]: handler attempt %d/%d failed event_id=%s: %v (retrying in %s)",
			c.groupID, attempt, maxRetries, env.EventID, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return lastErr
}

// Close stops the reader and waits for in-flight fetches to drain.
// Idempotent — calling Close twice is safe.
func (c *Consumer) Close() error {
	if c.reader == nil {
		return nil
	}
	r := c.reader
	c.reader = nil
	if err := r.Close(); err != nil {
		return fmt.Errorf("kafka.Consumer.Close: %w", err)
	}
	return nil
}

var _ broker.Consumer = (*Consumer)(nil)
