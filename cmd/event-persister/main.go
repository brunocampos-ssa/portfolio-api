// event-persister consumes wallet events from Kafka and writes them to
// the wallet_events table.
//
// Architecture:
//
//   wallet.events.v1 (Kafka topic)
//        │
//        ▼  consumer group: wallet-events-persister
//   ┌─────────────────┐
//   │  kafka.Consumer │  (FetchMessage → Unmarshal → handler → CommitMessages)
//   └────────┬────────┘
//            │  *broker.EventEnvelope
//            ▼
//   ┌─────────────────┐
//   │ persister.NewH  │  (envelope → domain.WalletEvent → repo.Create)
//   └────────┬────────┘
//            │
//            ▼
//   ┌─────────────────┐
//   │  Postgres       │  ON CONFLICT (id) DO NOTHING — idempotent.
//   │  wallet_events  │
//   └─────────────────┘
//
// At-least-once delivery from Kafka is absorbed by the row's primary
// key (the envelope's deterministic EventID): a duplicate delivery
// produces ON CONFLICT, the handler returns nil, the consumer commits
// the offset, life goes on.
//
// Usage:
//   go run ./cmd/event-persister
//
// Environment variables:
//   DATABASE_URL  - PostgreSQL connection string
//   KAFKA_BROKERS - Comma-separated Kafka bootstrap server list
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerkafka "github.com/brunocampos-ssa/portfolio-api/internal/broker/kafka"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/persister"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("FATAL: open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("FATAL: ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	eventRepo := postgres.NewEventRepository(db)
	handler := persister.NewHandler(eventRepo)

	consumer, err := brokerkafka.NewConsumer(
		cfg.KafkaBrokers,
		broker.KafkaTopicWalletEvents,
		broker.KafkaGroupPersister,
	)
	if err != nil {
		log.Fatalf("FATAL: kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("warning: kafka consumer close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Starting event persister...")
	log.Printf("  Kafka brokers: %v", cfg.KafkaBrokers)
	log.Printf("  Topic:         %s", broker.KafkaTopicWalletEvents)
	log.Printf("  Group ID:      %s", broker.KafkaGroupPersister)

	if err := consumer.Run(ctx, handler); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("FATAL: consumer error: %v", err)
	}

	log.Println("Event persister stopped.")
}
