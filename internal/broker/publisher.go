package broker

import "context"

// Publisher is the abstraction the watcher's pipeline depends on. The
// watcher does NOT know whether it's writing to Kafka, RabbitMQ, or a
// no-op test stub — it only sees Publish.
//
// This is what lets the same watcher run unchanged in:
//
//   - Production: KafkaPublisher to the cluster.
//   - Integration tests: KafkaPublisher to a testcontainers Kafka.
//   - Unit tests: an in-memory implementation that records calls.
//
// Implementations MUST:
//   - Be safe for concurrent use (the watcher publishes from one goroutine
//     today, but the contract has to allow scaling to N).
//   - Block until the broker has accepted the message OR the context is
//     cancelled. We deliberately do NOT offer a fire-and-forget mode; the
//     watcher relies on backpressure to slow polling when the broker
//     can't keep up.
//   - Return any non-nil error verbatim. Wrapping is the caller's job.
type Publisher interface {
	// Publish writes the envelope to the broker. Returns nil on
	// successful broker acknowledgement.
	Publish(ctx context.Context, env *EventEnvelope) error

	// Close releases any resources held by the publisher (TCP
	// connections, batch buffers, ...). After Close, further calls to
	// Publish MUST return an error rather than silently no-op'ing.
	Close() error
}

// Consumer is the symmetric abstraction for the read side. Each binary
// in cmd/event-* takes a Consumer and a handler function. The handler
// processes one envelope at a time and returns an error to indicate
// the message should be retried (Kafka: don't commit offset; RabbitMQ:
// nack with requeue).
//
// Class 2 lesson: handlers MUST be idempotent. The persister keys on
// (tx_hash, wallet_id, direction); the router stamps a delivery_id in
// RabbitMQ headers so downstream notifiers can dedupe.
type Consumer interface {
	// Run blocks until ctx is cancelled or an unrecoverable error
	// occurs, calling handler for every received envelope. Returning
	// a non-nil error from handler is the signal to retry — the
	// implementation chooses how (offset rewind, nack, etc.).
	Run(ctx context.Context, handler Handler) error

	// Close releases broker resources. Idempotent.
	Close() error
}

// Handler is the per-message callback Consumers invoke. Returning nil
// commits the message; returning an error triggers redelivery per the
// implementation's retry policy.
type Handler func(ctx context.Context, env *EventEnvelope) error
