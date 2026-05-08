// event-analytics is a Kafka consumer (group: analytics) that maintains
// in-memory aggregations across the event stream — total transfers per
// token, per direction, per network, per hour. Equivalent to Class 1's
// metricsWorker, lifted out of the watcher process so it can be scaled
// and restarted independently.
//
// Why a separate consumer group? Because analytics replays history on
// startup (StartOffset = FirstOffset) — each restart recomputes the
// running totals from t=0. The persister and router don't want that
// behavior; their groups commit offsets and resume. Same topic, three
// different read patterns — that's why consumer groups exist.
//
// Class 2 will replace this stub with:
//
//   - kafka.NewConsumer(brokers, broker.KafkaTopicWalletEvents,
//                       broker.KafkaGroupAnalytics)
//   - consumer.Run(ctx, agg.Apply) where agg is an in-memory
//     incoming/outgoing tracker with a periodic stdout dump.
package main

import "log"

func main() {
	log.Fatal("event-analytics: not implemented yet (Class 2 stub)")
}
