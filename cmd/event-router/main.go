// event-router bridges Kafka (source of truth) to RabbitMQ (topic-
// routed notifications). It consumes wallet.events.v1 on its own
// consumer group and re-publishes each envelope into the
// wallet.events topic exchange with a routing key derived from
// (network, direction, token).
//
// Architecture:
//
//   wallet.events.v1 (Kafka topic)
//        │
//        ▼  consumer group: wallet-events-router
//   ┌─────────────────┐
//   │  kafka.Consumer │
//   └────────┬────────┘
//            │  *broker.EventEnvelope
//            ▼
//   ┌─────────────────┐
//   │ router.NewHand. │
//   └────────┬────────┘
//            │
//            ▼  routing key: <network>.<direction>.<token>
//   ┌─────────────────┐
//   │ rabbitmq.Pub    │  Mandatory=true, confirm.select
//   └────────┬────────┘
//            │
//            ▼
//   wallet.events exchange (topic)
//            │
//            ▼
//   queues bound by event-notifier and friends
//
// Two transports, one event identity. Notifiers subscribed to a
// pattern don't care that the event came from Kafka; they just see
// it in their queue.
//
// Usage:
//   go run ./cmd/event-router
//
// Environment variables:
//   KAFKA_BROKERS - Comma-separated Kafka bootstrap server list
//   RABBITMQ_URL  - AMQP URL (amqp://user:pass@host:port/)
package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	brokerrabbit "github.com/brunocampos-ssa/portfolio-api/internal/broker/rabbitmq"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	// --- RabbitMQ publisher (the bridge's output side) ---
	rabbitPub, err := brokerrabbit.NewPublisher(cfg.RabbitMQURL, broker.RabbitExchangeWalletEvents)
	if err != nil {
		log.Fatalf("FATAL: rabbitmq publisher: %v", err)
	}
	defer func() {
		if err := rabbitPub.Close(); err != nil {
			log.Printf("warning: rabbitmq publisher close: %v", err)
		}
	}()
	log.Printf("RabbitMQ publisher ready: url=%s exchange=%s",
		cfg.RabbitMQURL, broker.RabbitExchangeWalletEvents)

	// --- Kafka consumer (the bridge's input side) ---
	kafkaCons, err := brokerkafka.NewConsumer(
		cfg.KafkaBrokers,
		broker.KafkaTopicWalletEvents,
		broker.KafkaGroupRouter,
	)
	if err != nil {
		log.Fatalf("FATAL: kafka consumer: %v", err)
	}
	defer func() {
		if err := kafkaCons.Close(); err != nil {
			log.Printf("warning: kafka consumer close: %v", err)
		}
	}()

	// --- Bridge handler: trivial body, all behavior in the
	//     publisher/consumer adapters. That separation is the
	//     point. ---
	handler := router.NewHandler(rabbitPub)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Starting event router...")
	log.Printf("  Kafka brokers:   %v", cfg.KafkaBrokers)
	log.Printf("  Kafka topic:     %s", broker.KafkaTopicWalletEvents)
	log.Printf("  Kafka group:     %s", broker.KafkaGroupRouter)
	log.Printf("  Rabbit exchange: %s", broker.RabbitExchangeWalletEvents)

	if err := kafkaCons.Run(ctx, handler); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("FATAL: consumer error: %v", err)
	}

	log.Println("Event router stopped.")
}
