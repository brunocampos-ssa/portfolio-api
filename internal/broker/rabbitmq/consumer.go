package rabbitmq

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
)

// Consumer subscribes to a queue bound to the topic exchange with a
// routing-key pattern, e.g. "*.incoming.*".
//
// TODO Class 2: declare the queue (durable, named after the consumer),
// bind it to broker.RabbitExchangeWalletEvents with bindingKey, set
// QoS prefetch=N (so a slow handler can't accumulate unbounded), and
// loop over deliveries from ch.Consume invoking the handler. On
// handler == nil call delivery.Ack(false); on error call delivery.Nack(
// false, true) for requeue or DLQ-route after N attempts (header
// "x-death" tracks redelivery count).
type Consumer struct {
	url        string
	exchange   string
	queueName  string
	bindingKey string

	// conn / ch will hold the AMQP connection and channel after Class
	// 2 wires them up. nil in the skeleton.
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewConsumer wires a consumer to the given exchange via a queue named
// queueName, bound with bindingKey (an AMQP topic pattern).
//
// All four arguments are required. Class 2 will document why named
// queues over server-generated names: durability across consumer
// restarts and shareable identity for monitoring.
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
	return &Consumer{
		url:        url,
		exchange:   exchange,
		queueName:  queueName,
		bindingKey: bindingKey,
	}, nil
}

// Run consumes deliveries until ctx is cancelled. Handler errors trigger
// nack + requeue.
func (c *Consumer) Run(_ context.Context, _ broker.Handler) error {
	return errors.New("rabbitmq.Consumer.Run: not implemented (Class 2 stub)")
}

// Close shuts down the channel and the connection.
func (c *Consumer) Close() error {
	return nil
}

var _ broker.Consumer = (*Consumer)(nil)
