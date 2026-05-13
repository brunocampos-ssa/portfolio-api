// Package notifier is the business logic of cmd/event-notifier: a
// broker.Handler that invokes a "notify" callback for every delivered
// envelope. The callback is what differs across deployments —
// production pushes to Slack/email/PagerDuty, tests capture into a
// channel, Class 2's cmd binary logs to stdout.
//
// The notifier is the last hop in the messaging chain:
//
//   ... → RabbitMQ topic exchange → queue (bound by pattern) →
//   notifier handler → side effect
//
// Two Class 2 teaching points come together here:
//
//   1. The broker.Handler interface is BROKER-AGNOSTIC. event-persister
//      runs this exact contract behind a kafka.Consumer; the notifier
//      runs it behind a rabbitmq.Consumer. The handler doesn't know
//      and shouldn't care.
//
//   2. At-least-once is the norm. The router can re-deliver the same
//      envelope after a crash; the notifier MUST be idempotent on
//      EventID if its side-effect is user-visible. Class 2 skips the
//      dedup cache (we just log) and notes the design intent. A real
//      Slack-pushing notifier would maintain an LRU of recent EventIDs
//      or rely on idempotency keys at the destination.
package notifier

import (
	"context"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Notify is the per-event side effect. Returning an error triggers the
// consumer's bounded retry; nil acks the message.
//
// Implementations SHOULD be idempotent over EventID if their side
// effect is user-visible. A re-delivery from RabbitMQ is indistinguish-
// able from a fresh event at this layer.
type Notify func(ctx context.Context, env *broker.EventEnvelope) error

// NewHandler returns a broker.Handler that calls notify for each
// envelope. Trivial body — the same shape as event-router's NewHandler
// — because the broker abstraction earned its keep: kafka.Consumer
// and rabbitmq.Consumer both drive this with no notifier-side changes.
//
// Returning nil for a nil envelope mirrors the persister/router
// contract: defensive behaviour for a kind of bug the consumers don't
// actually produce, but the handler shouldn't trust them blindly.
func NewHandler(notify Notify) broker.Handler {
	if notify == nil {
		panic("notifier.NewHandler: notify must not be nil")
	}
	return func(ctx context.Context, env *broker.EventEnvelope) error {
		if env == nil {
			return nil
		}
		if err := notify(ctx, env); err != nil {
			// Wrap with context so the consumer's "exhausted retries"
			// log line names the notify operation rather than just
			// the underlying transport.
			return fmt.Errorf("notifier: notify event_id=%s: %w", env.EventID, err)
		}
		return nil
	}
}
