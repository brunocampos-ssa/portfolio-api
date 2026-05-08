// event-persister consumes wallet events from Kafka and writes them to
// the wallet_events table. It runs as part of consumer group
// broker.KafkaGroupPersister, so it has its own offset and can be
// scaled out (multiple replicas → partition assignment).
//
// Class 2 will replace this stub with the real wire-up:
//
//   - sql.Open(DATABASE_URL)
//   - postgres.NewEventRepository(db)
//   - kafka.NewConsumer(brokers, broker.KafkaTopicWalletEvents, broker.KafkaGroupPersister)
//   - consumer.Run(ctx, func(ctx, env) error {
//         return eventRepo.Create(ctx, envelopeToDomain(env))
//     })
//
// Idempotency: the repository's INSERT relies on the wallet_events
// (tx_hash, wallet_id, direction) UNIQUE constraint. A re-delivery from
// Kafka (offset commit raced with handler completion) returns a unique-
// violation, which the persister will swallow as "already persisted".
package main

import "log"

func main() {
	log.Fatal("event-persister: not implemented yet (Class 2 stub)")
}
