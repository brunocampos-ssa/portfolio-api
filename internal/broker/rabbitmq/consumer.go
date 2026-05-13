package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// =============================================================================
// Consumer
// =============================================================================
//
// RabbitMQ consumer model is queue-centric: declare an exchange,
// declare a queue, bind the queue to the exchange with a routing-key
// pattern, then read deliveries off the queue. Different queues bound
// to the same exchange under different patterns is what gives the
// "subscribe to a slice of the firehose" property — that's the
// headline RabbitMQ teaching point in Class 2.
//
// Configuration choices, all teaching points:
//
//   - Durable named queue. The queue survives broker restart AND keeps
//     its identity across consumer process restarts. A server-named
//     temporary queue would lose its bindings (and any unacked
//     messages) every time the consumer reconnects.
//
//   - Topic exchange + AMQP routing-key patterns. "*" matches one
//     segment, "#" matches zero or more. Examples:
//       *.incoming.*  matches "ethereum.incoming.usdc" but not
//                     "ethereum.outgoing.usdc"
//       *.*.usdc      matches USDC events on any chain, any direction
//       ethereum.#    matches every Ethereum event
//
//   - QoS prefetch=10. The broker stops sending more deliveries to
//     this consumer once 10 unacked are outstanding. Keeps the
//     handler's backlog bounded — without prefetch a slow handler
//     would accumulate the broker's entire queue in this process's
//     memory.
//
//   - Manual ack (autoAck=false). The handler runs BEFORE the ack,
//     same as kafka.Consumer. On success: Ack. After bounded retry
//     exhaustion: Nack(requeue=false) so a future DLX exchange
//     catches it (we don't configure a DLX here — Class 2 keeps it
//     simple — but the call shape is in place).
//
// Class 2 lesson, mirrored from kafka.Consumer's docstring:
// idempotency is the HANDLER's responsibility. RabbitMQ requeue-
// based redelivery (and Kafka's at-least-once) both depend on
// handlers absorbing duplicates without producing wrong results.

// Consumer subscribes to a queue bound to the topic exchange with a
// routing-key pattern, e.g. "*.incoming.*".
type Consumer struct {
	url        string
	exchange   string
	queueName  string
	bindingKey string

	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewConsumer dials the broker, opens a channel, declares the
// exchange + queue, binds them, and sets QoS. All four arguments are
// required.
//
// queueName is intentionally a parameter rather than auto-generated:
// named queues persist their identity across consumer restarts, which
// is required for resuming consumption from where we left off.
// Different consumer processes that want to share the work get the
// SAME queueName; consumers that want independent views get DIFFERENT
// queueNames. (RabbitMQ has no consumer-group concept — multiple
// consumers reading from the same queue compete for messages, which
// is the equivalent.)
func NewConsumer(url, exchange, queueName, bindingKey string) (*Consumer, error) {
	if url == "" {
		return nil, errors.New("rabbitmq.NewConsumer: url must not be empty")
	}
	if exchange == "" {
		return nil, errors.New("rabbitmq.NewConsumer: exchange must not be empty")
	}
	if queueName == "" {
		return nil, errors.New("rabbitmq.NewConsumer: queueName must not be empty")
	}
	if bindingKey == "" {
		return nil, errors.New("rabbitmq.NewConsumer: bindingKey must not be empty")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq.NewConsumer: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewConsumer: channel: %w", err)
	}

	if err := ch.ExchangeDeclare(exchange, "topic",
		true, false, false, false, nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewConsumer: declare exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete: keep around when no consumers
		false, // exclusive: shared across processes
		false, // noWait
		nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewConsumer: declare queue: %w", err)
	}

	if err := ch.QueueBind(queueName, bindingKey, exchange, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewConsumer: bind queue: %w", err)
	}

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq.NewConsumer: qos: %w", err)
	}

	return &Consumer{
		url:        url,
		exchange:   exchange,
		queueName:  queueName,
		bindingKey: bindingKey,
		conn:       conn,
		ch:         ch,
	}, nil
}

// Run drives the consume loop until ctx is cancelled. Returns ctx.Err()
// on graceful shutdown.
//
// Same handler-with-retry contract as kafka.Consumer:
//
//   - Schema decode error → Nack(requeue=false). Poison-pill
//     protection: requeueing would just route it back to us forever.
//   - Handler success → Ack(multiple=false).
//   - Handler exhausts retries → Nack(requeue=false). A DLX (when
//     configured) catches it; otherwise the broker drops it. Either
//     way the queue keeps moving — same trade-off as Kafka.
//   - ctx cancellation → cancel the consumer at the broker, drain
//     any in-flight deliveries (without acking — broker requeues
//     them on reconnect), then return ctx.Err().
//
// Concurrency: Run MUST be called from a single goroutine.
// amqp091-go channels are not safe for concurrent ack/nack.
//
// Implementation note: we use plain Consume (not ConsumeWithContext)
// because ConsumeWithContext interleaves its own basic.cancel with
// our Close path and can deadlock on Channel.Close. Managing the
// consumer's lifecycle explicitly here keeps the AMQP frame ordering
// predictable.
func (c *Consumer) Run(ctx context.Context, handler broker.Handler) error {
	if c.ch == nil {
		return errors.New("rabbitmq.Consumer.Run: consumer is closed")
	}
	if handler == nil {
		return errors.New("rabbitmq.Consumer.Run: handler must not be nil")
	}

	// consumerTag identifies this consumer to the broker — useful
	// for the management UI's "consumers" view, AND for the explicit
	// basic.cancel we send on shutdown.
	consumerTag := "consumer-" + c.queueName
	deliveries, err := c.ch.Consume(
		c.queueName,
		consumerTag,
		false, // autoAck=false: we ack/nack explicitly
		false, // exclusive=false: shared queue
		false, // noLocal: AMQP requires this; broker ignores
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq.Consumer.Run: consume: %w", err)
	}
	// On exit, send basic.cancel so the broker stops pushing new
	// deliveries to this consumer-tag. noWait=true makes this a
	// fire-and-forget frame: the goroutine exits immediately rather
	// than blocking for the broker's basic.cancel-ok. This matters
	// on shutdown because Close() is racing us to acquire the
	// channel's internal RPC slot — waiting here would let Close
	// queue up a channel.close that gets stuck behind us.
	defer func() {
		_ = c.ch.Cancel(consumerTag, true /* noWait */)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case d, ok := <-deliveries:
			if !ok {
				// Channel closed by the broker (connection drop or
				// the channel being closed by Close). Treat as
				// shutdown — the operator restarts us.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("rabbitmq.Consumer.Run: deliveries channel closed unexpectedly")
			}

			env, err := broker.Unmarshal(d.Body)
			if err != nil {
				log.Printf("rabbitmq.Consumer[%s]: skip undecodable delivery routing_key=%s: %v",
					c.queueName, d.RoutingKey, err)
				// Don't requeue a poison pill — that would just route
				// it right back to us.
				if nackErr := d.Nack(false /* multiple */, false /* requeue */); nackErr != nil {
					log.Printf("rabbitmq.Consumer[%s]: nack poison pill: %v", c.queueName, nackErr)
				}
				continue
			}

			if err := c.invokeWithRetry(ctx, handler, env); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("rabbitmq.Consumer[%s]: handler exhausted retries event_id=%s: %v",
					c.queueName, env.EventID, err)
				if nackErr := d.Nack(false, false); nackErr != nil {
					log.Printf("rabbitmq.Consumer[%s]: nack: %v", c.queueName, nackErr)
				}
				continue
			}

			if ackErr := d.Ack(false); ackErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("rabbitmq.Consumer.Run: ack: %w", ackErr)
			}
		}
	}
}

// invokeWithRetry calls handler up to maxRetries times with
// exponential backoff between attempts. Mirrors kafka.Consumer's
// retry shape so handlers can be moved between brokers without
// changing their failure semantics.
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
		log.Printf("rabbitmq.Consumer[%s]: handler attempt %d/%d failed event_id=%s: %v (retrying in %s)",
			c.queueName, attempt, maxRetries, env.EventID, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return lastErr
}

// Close shuts down the channel and the underlying TCP connection.
// Idempotent.
func (c *Consumer) Close() error {
	if c.ch == nil && c.conn == nil {
		return nil
	}
	var firstErr error
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			firstErr = fmt.Errorf("rabbitmq.Consumer.Close: channel: %w", err)
		}
		c.ch = nil
	}
	if c.conn != nil {
		// Always attempt the connection close even if the channel
		// close failed; only RECORD the connection error if we
		// don't already have one (first-error-wins).
		if err := c.conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("rabbitmq.Consumer.Close: connection: %w", err)
		}
		c.conn = nil
	}
	return firstErr
}

// Tunables shared with kafka.Consumer (same retry shape; the package
// can't import broker/kafka without a cycle so we duplicate the
// constants here).
const (
	maxRetries     = 3
	initialBackoff = 200 * time.Millisecond
	prefetchCount  = 10
)

var _ broker.Consumer = (*Consumer)(nil)
