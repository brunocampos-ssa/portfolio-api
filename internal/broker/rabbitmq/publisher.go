// Package rabbitmq is the rabbitmq/amqp091-go adapter for
// broker.Publisher and broker.Consumer.
//
// Used by event-router (publishes filtered envelopes from Kafka into a
// topic exchange) and event-notifier (subscribes to a routing-key
// pattern). The watcher does NOT use this package — Kafka is the
// source of truth in Class 2's topology.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// =============================================================================
// Publisher
// =============================================================================
//
// RabbitMQ does not have Kafka's "broker writes to disk and acks"
// semantics by default — basic.publish is fire-and-forget. To get
// genuinely durable, ack'd publishes we have to opt into TWO things:
//
//  1. confirm.select on the channel — the broker now acks every
//     publish, asynchronously. amqp091-go exposes that as deferred
//     confirmations: PublishWithDeferredConfirmWithContext returns a
//     handle whose Wait() blocks until the broker (n)acks. This is
//     the analogue of kafka-go's RequiredAcks=RequireAll.
//
//  2. Mandatory=true + NotifyReturn — if the message routes to no
//     queue, RabbitMQ "returns" it asynchronously. Without the return
//     listener those messages disappear silently — exactly the kind
//     of broker misconfiguration that surfaces at 3am. We register a
//     return listener and log loudly. Returning is NOT the same as
//     a NACK: the broker accepted the message, just had nowhere to
//     put it.
//
// Combined, this gives "publish blocks until the broker has either
// ack'd or returned the message" semantics — strong enough to back
// the event-router's at-least-once handoff from Kafka.

// Publisher writes envelopes into a RabbitMQ topic exchange. The
// routing key is derived from the envelope via broker.RoutingKey.
type Publisher struct {
	url      string
	exchange string

	conn *amqp.Connection
	ch   *amqp.Channel

	// returns is the NotifyReturn channel — populated by the broker
	// when Mandatory=true and the message routes to zero queues.
	// Drained by a background goroutine that just logs.
	returns chan amqp.Return

	// publishMu serialises Publish calls. amqp091-go channels are not
	// safe for concurrent publishing, and serialising here is
	// simpler than maintaining a per-goroutine channel pool.
	publishMu sync.Mutex

	// closed flips on Close. Subsequent Publish calls error out
	// instead of panicking on a closed channel.
	closed bool

	// publishTimeout caps how long we wait for the broker's
	// confirm. RabbitMQ network latencies >5s usually mean an outage,
	// not slowness; failing fast lets the caller (event-router)
	// retry rather than block its consume loop.
	publishTimeout time.Duration
}

// NewPublisher dials the broker, opens a channel, declares the
// exchange (idempotent), enables confirm mode, and registers the
// return listener. Construction is eager — failures here mean the
// caller (cmd/event-router or cmd/event-notifier) crashes at startup
// rather than at first publish.
func NewPublisher(url, exchange string) (*Publisher, error) {
	if url == "" {
		return nil, errors.New("rabbitmq.NewPublisher: url must not be empty")
	}
	if exchange == "" {
		return nil, errors.New("rabbitmq.NewPublisher: exchange must not be empty")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq.NewPublisher: dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewPublisher: channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		exchange,
		"topic",
		true,  // durable: survives broker restart.
		false, // autoDelete: keep around even when no queues are bound.
		false, // internal: regular exchange, not internal-only.
		false, // noWait: wait for the broker's ack of the declare.
		nil,   // args
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewPublisher: declare exchange: %w", err)
	}

	if err := ch.Confirm(false /* noWait */); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewPublisher: enable confirm mode: %w", err)
	}

	p := &Publisher{
		url:            url,
		exchange:       exchange,
		conn:           conn,
		ch:             ch,
		returns:        make(chan amqp.Return, 16),
		publishTimeout: 5 * time.Second,
	}
	ch.NotifyReturn(p.returns)
	go p.drainReturns()

	return p, nil
}

// Publish sends an envelope to the topic exchange under the routing
// key derived from broker.RoutingKey(env). Blocks until the broker
// confirms or the publishTimeout / ctx fires.
//
// Returns an error if the broker NACKs the message (the only case
// where the message did NOT make it to a durable queue) or if the
// confirmation times out. Mandatory-returns are NOT surfaced as
// errors — they're logged via the return listener. This matches
// reality: a returned message means "you're publishing to a
// routing key with no queue bound", which is a deployment bug to
// fix, not a transient retry condition.
func (p *Publisher) Publish(ctx context.Context, env *broker.EventEnvelope) error {
	if env == nil {
		return errors.New("rabbitmq.Publisher.Publish: envelope must not be nil")
	}

	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	if p.closed {
		return errors.New("rabbitmq.Publisher.Publish: publisher is closed")
	}

	body, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("rabbitmq.Publisher.Publish: marshal: %w", err)
	}

	routingKey := broker.RoutingKey(env)

	pubCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		pubCtx,
		p.exchange,
		routingKey,
		true,  // mandatory: undeliverable messages are returned, not silently dropped.
		false, // immediate: deprecated, must be false on modern brokers.
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survive broker restart.
			Timestamp:    env.EmittedAt,
			MessageId:    env.EventID,
			Headers: amqp.Table{
				"schema_version": int32(env.SchemaVersion),
				"event_id":       env.EventID,
			},
			Body: body,
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq.Publisher.Publish: publish %s: %w", routingKey, err)
	}

	// Wait for the broker's ack/nack. Done() is closed when the
	// confirmation arrives; ctx covers timeout/cancellation.
	select {
	case <-confirmation.Done():
		if !confirmation.Acked() {
			return fmt.Errorf("rabbitmq.Publisher.Publish: broker NACK for routing key %s", routingKey)
		}
	case <-pubCtx.Done():
		return fmt.Errorf("rabbitmq.Publisher.Publish: confirmation %s: %w", routingKey, pubCtx.Err())
	}
	return nil
}

// Close shuts down the channel and the underlying TCP connection.
// Idempotent.
func (p *Publisher) Close() error {
	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			firstErr = fmt.Errorf("rabbitmq.Publisher.Close: channel: %w", err)
		}
	}
	if p.conn != nil {
		// Always attempt the connection close even if the channel
		// close failed; only RECORD the connection error if we
		// don't already have one (first-error-wins).
		if err := p.conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("rabbitmq.Publisher.Close: connection: %w", err)
		}
	}
	return firstErr
}

// drainReturns consumes the NotifyReturn channel and logs each
// returned message. Returns happen when Mandatory=true and the
// broker can't route the message — almost always a deployment bug
// (no queue bound to the routing key pattern). Logging loudly puts
// it in the operator's lap rather than letting messages vanish.
//
// The goroutine exits when the channel is closed by Close() (which
// happens implicitly when ch.Close() runs).
func (p *Publisher) drainReturns() {
	for r := range p.returns {
		log.Printf("rabbitmq: BROKER RETURN — no queue bound for routing_key=%s reason=%s message_id=%s",
			r.RoutingKey, r.ReplyText, r.MessageId)
	}
}

var _ broker.Publisher = (*Publisher)(nil)
