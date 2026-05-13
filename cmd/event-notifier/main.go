// event-notifier subscribes to the wallet.events topic exchange on
// RabbitMQ and "alerts" for every matching envelope. The binding
// pattern is configurable via NOTIFIER_BINDING_KEY so the same
// binary serves every alerting use case:
//
//   *.incoming.*  — alert on any incoming transfer, any chain, any token
//   *.*.usdc      — alert on USDC events, any chain, any direction
//   ethereum.#    — alert on every Ethereum event
//
// Class 2's notify is a stdout log line. A production alerting
// implementation would push to Slack, email, or PagerDuty here —
// the notifier package's Notify type is the seam.
//
// Architecture:
//
//   wallet.events exchange (topic)
//        │
//        │  routing key: <network>.<direction>.<token>
//        ▼
//   queue: event-notifier
//   bound by: NOTIFIER_BINDING_KEY (default *.incoming.*)
//        │
//        ▼
//   ┌─────────────────┐
//   │ rabbitmq.Consumer│  prefetch=10, manual ack/nack
//   └────────┬─────────┘
//            │  *broker.EventEnvelope
//            ▼
//   ┌─────────────────┐
//   │ notifier.NewH.  │
//   └────────┬─────────┘
//            ▼  stdout (or Slack/PagerDuty in prod)
//
// Idempotency note: the bridge (event-router) can re-deliver the same
// envelope after a crash. A real Slack-pushing notifier MUST dedupe
// on EventID — typically an LRU of recent IDs or an idempotency-key
// header at the destination. Class 2's stdout-logging notifier accepts
// duplicates; we just note them.
//
// Usage:
//   go run ./cmd/event-notifier
//
// Environment variables:
//   RABBITMQ_URL         - AMQP URL (amqp://user:pass@host:port/)
//   NOTIFIER_BINDING_KEY - routing-key pattern (default *.incoming.*)
//   NOTIFIER_QUEUE_NAME  - durable queue name (default event-notifier)
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	brokerrabbit "github.com/brunocampos-ssa/portfolio-api/internal/broker/rabbitmq"
	"github.com/brunocampos-ssa/portfolio-api/internal/config"
	"github.com/brunocampos-ssa/portfolio-api/internal/notifier"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("FATAL: load config: %v", err)
	}

	bindingKey := getEnv("NOTIFIER_BINDING_KEY", "*.incoming.*")
	queueName := getEnv("NOTIFIER_QUEUE_NAME", "event-notifier")

	consumer, err := brokerrabbit.NewConsumer(
		cfg.RabbitMQURL,
		broker.RabbitExchangeWalletEvents,
		queueName,
		bindingKey,
	)
	if err != nil {
		log.Fatalf("FATAL: rabbitmq consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("warning: rabbitmq consumer close: %v", err)
		}
	}()

	// Class 2's notify: log to stdout. Real deployments swap this
	// for a Slack/email/PagerDuty client. Keep the body small —
	// anything heavier (retries, batching, formatting) belongs in
	// the destination client, not here, because the consumer's
	// bounded retry already covers transient failures.
	notify := func(_ context.Context, env *broker.EventEnvelope) error {
		log.Printf("notifier[%s]: ALERT event_id=%s network=%s direction=%s token=%s amount=%s tx=%s block=%d wallet=%s",
			bindingKey,
			env.EventID, env.Network, env.Direction, env.TokenSymbol,
			env.Amount, env.TxHash, env.BlockNumber, env.WalletID)
		return nil
	}

	handler := notifier.NewHandler(notify)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Starting event notifier...")
	log.Printf("  RabbitMQ URL: %s", cfg.RabbitMQURL)
	log.Printf("  Exchange:     %s", broker.RabbitExchangeWalletEvents)
	log.Printf("  Queue:        %s", queueName)
	log.Printf("  Binding key:  %s", bindingKey)

	if err := consumer.Run(ctx, handler); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("FATAL: consumer error: %v", err)
	}

	log.Println("Event notifier stopped.")
}

// getEnv mirrors the helper in internal/config but stays local so this
// cmd doesn't reach across packages for a 6-line utility.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
