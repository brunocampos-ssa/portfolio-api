// event-notifier consumes from a RabbitMQ queue bound to the
// wallet.events topic exchange and reacts to events that match its
// binding pattern. Class 2's notifier just logs to stdout; a real
// implementation would push to email/Slack/PagerDuty.
//
// The binding pattern is configurable via NOTIFIER_BINDING_KEY (default
// "*.incoming.*" — any incoming transfer on any chain for any token).
// That's the headline RabbitMQ teaching moment: one exchange, many
// queues, each subscribing to a different slice of the firehose
// without any producer awareness.
//
// Class 2 will replace this stub with:
//
//   - rabbitmq.NewConsumer(url, broker.RabbitExchangeWalletEvents,
//                          "event-notifier", os.Getenv("NOTIFIER_BINDING_KEY"))
//   - consumer.Run(ctx, func(ctx, env) error { log.Printf(...); return nil })
package main

import "log"

func main() {
	log.Fatal("event-notifier: not implemented yet (Class 2 stub)")
}
