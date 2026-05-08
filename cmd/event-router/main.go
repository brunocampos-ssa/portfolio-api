// event-router is the bridge between Kafka (source of truth) and
// RabbitMQ (topic-routed notifications). It consumes the same wallet
// events stream as the persister but on its own consumer group, then
// re-publishes each envelope into the wallet.events topic exchange
// with a routing key derived from the envelope.
//
// This pattern (single source of truth + downstream forwarders) is what
// real systems usually look like: the durable log of record is one
// system, fan-out to "interesting subset" subscribers is another. We
// split it explicitly so students see why — coupling the watcher to
// both brokers would mean the watcher fails when either is down.
//
// Class 2 will replace this stub with:
//
//   - kafka.NewConsumer(brokers, broker.KafkaTopicWalletEvents, broker.KafkaGroupRouter)
//   - rabbitmq.NewPublisher(url, broker.RabbitExchangeWalletEvents)
//   - consumer.Run(ctx, func(ctx, env) error {
//         return rabbitPublisher.Publish(ctx, env)  // routing key derived inside
//     })
//
// Idempotency: the notifier's queue may receive duplicates if the router
// crashes between the publish and the Kafka offset commit. The notifier
// is responsible for de-duping on EventID (we propagate it as an AMQP
// header so the notifier can keep an LRU cache without parsing the body).
package main

import "log"

func main() {
	log.Fatal("event-router: not implemented yet (Class 2 stub)")
}
