// Package rabbitmq is the rabbitmq/amqp091-go adapter for
// broker.Publisher and broker.Consumer.
//
// Used by event-router (publishes filtered envelopes from Kafka into a
// topic exchange) and event-notifier (subscribes to a routing-key
// pattern). The watcher does NOT use this package — Kafka is the
// source of truth in Class 2's topology.
//
// Class 2 will fill these stubs in.
package rabbitmq

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Publisher writes envelopes into a RabbitMQ topic exchange. The routing
// key is derived from the envelope via broker.RoutingKey.
//
// TODO Class 2: hold an amqp091.Connection + amqp091.Channel; on
// Publish, marshal the envelope and call ch.PublishWithContext with:
//   - Mandatory = true so the broker returns the message if no queue is
//     bound to the routing key — silent drops are an antipattern.
//   - DeliveryMode = Persistent so messages survive broker restarts.
//   - Headers carrying the event_id and schema_version for downstream
//     dedupe and version negotiation.
type Publisher struct {
	// url is an AMQP URL like "amqp://user:pass@host:5672/vhost".
	url string

	// exchange is the topic exchange name. Class 2 uses
	// broker.RabbitExchangeWalletEvents.
	exchange string

	// conn / ch are the AMQP connection and channel. nil in the
	// skeleton; Class 2 will dial in NewPublisher and lazily declare
	// the exchange before the first Publish.
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewPublisher constructs a RabbitMQ publisher. url and exchange must
// both be non-empty. Class 2 will lazily declare the exchange (idempotent
// PassiveDeclare → Declare) so first-run is self-bootstrapping.
func NewPublisher(url, exchange string) (*Publisher, error) {
	if url == "" {
		return nil, errors.New("rabbitmq.NewPublisher: url must not be empty")
	}
	if exchange == "" {
		return nil, errors.New("rabbitmq.NewPublisher: exchange must not be empty")
	}
	return &Publisher{url: url, exchange: exchange}, nil
}

// Publish sends an envelope to the topic exchange under its derived
// routing key.
func (p *Publisher) Publish(_ context.Context, _ *broker.EventEnvelope) error {
	return errors.New("rabbitmq.Publisher.Publish: not implemented (Class 2 stub)")
}

// Close shuts down the channel and the connection.
func (p *Publisher) Close() error {
	return nil
}

var _ broker.Publisher = (*Publisher)(nil)
