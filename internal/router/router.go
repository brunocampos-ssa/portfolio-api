// Package router is the business logic of cmd/event-router: a
// broker.Handler that re-publishes envelopes from the Kafka source-of-
// truth into a RabbitMQ topic exchange.
//
// Lives in internal/router so it can be unit-tested without spinning
// up either broker — the cmd/* binary is just wiring on top.
//
// The router is the bridge between two semantically different buses:
//
//   - Kafka is the durable, partitioned, replayable event log. Every
//     event lives forever (until retention), every consumer group
//     gets an independent view. The persister stores events; the
//     router forwards them; the analytics consumer aggregates them.
//     All on the SAME topic, different consumer groups.
//
//   - RabbitMQ is the topic-routed notification fabric. Queues bind
//     to the exchange with routing-key patterns. Notifiers subscribe
//     to a slice ("any incoming USDC", "any Ethereum event") without
//     coordinating with the producer. The router is the producer.
//
// Why a separate process for the bridge (and not have the watcher
// publish to both)? Because the watcher should fail-or-degrade on
// only one dependency. With the bridge as a separate service:
//
//   - Watcher down → no new events captured (acceptable degradation).
//   - Kafka down → watcher backpressures, but already-captured events
//     in earlier offsets are safe.
//   - RabbitMQ down → router backs off but Kafka keeps accumulating
//     events; the persister keeps working; when RabbitMQ recovers
//     the router catches up from its committed offset.
//   - Router down → same as above, except notifications lag.
//
// One bus down does not break any other consumer. That's the
// architectural value of the topology, and the router is what makes
// it real.
//
// Idempotency: at-least-once redelivery is the norm on both sides.
// Kafka can replay; RabbitMQ has no built-in dedup. The notifier
// MUST be idempotent on EventID — typically an LRU cache of recently-
// seen IDs.
package router

import (
	"context"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// NewHandler returns a broker.Handler that publishes each consumed
// envelope into the provided RabbitMQ publisher. Trivial body — the
// kafka.Consumer's bounded-retry absorbs transient RabbitMQ failures
// and the broker.RoutingKey derivation lives inside the rabbit
// publisher, so there's nothing for the router-level handler to do
// beyond forwarding.
//
// Returning an error triggers the consumer's retry. Nil envelopes
// commit as no-ops — same defensive contract as the persister.
func NewHandler(pub broker.Publisher) broker.Handler {
	if pub == nil {
		panic("router.NewHandler: pub must not be nil")
	}
	return func(ctx context.Context, env *broker.EventEnvelope) error {
		if env == nil {
			return nil
		}
		if err := pub.Publish(ctx, env); err != nil {
			// Wrap with context so the consumer's "exhausted
			// retries" log line names the bridge operation, not
			// just the underlying transport error.
			return fmt.Errorf("router: forward to rabbitmq: %w", err)
		}
		return nil
	}
}

