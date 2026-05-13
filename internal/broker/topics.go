package broker

// Topic / exchange / routing-key constants. Centralised so the producer,
// the router, and every consumer agree on naming. Versioned suffixes
// (...v1) leave room for an in-place migration when SchemaCurrent bumps.

const (
	// KafkaTopicWalletEvents is the Kafka topic populated by the watcher.
	// Partition key = wallet address (lowercased) so events for the same
	// wallet land on the same partition and stay ordered. Consumers that
	// need cross-wallet ordering can sort by (block_number, event_id).
	KafkaTopicWalletEvents = "wallet.events.v1"

	// RabbitExchangeWalletEvents is the topic exchange the router writes
	// into. The notifier (and any future RabbitMQ-side consumer) binds
	// queues to this exchange with a routing-key pattern.
	RabbitExchangeWalletEvents = "wallet.events"
)

// Kafka consumer group identifiers. Each group has its own offsets, so
// the persister, router, and analytics consumers each see the full
// stream independently. Adding a new consumer = adding a new group, no
// producer changes.
const (
	KafkaGroupPersister = "wallet-events-persister"
	KafkaGroupRouter    = "wallet-events-router"
	KafkaGroupAnalytics = "wallet-events-analytics"
)

// RoutingKey assembles the RabbitMQ routing key from the envelope.
// Format: <network>.<direction>.<token_lowercase>. Examples:
//
//	ethereum.incoming.usdc
//	ethereum.outgoing.eth
//	klever.incoming.klv
//
// Notifier subscribes with patterns like "*.incoming.*" (anything coming
// in, on any chain, for any token) or "ethereum.#" (everything Ethereum).
// The dot-separated structure is what enables AMQP topic-pattern matching.
func RoutingKey(env *EventEnvelope) string {
	return env.Network + "." + env.Direction + "." + lower(env.TokenSymbol)
}

// lower is intentionally a tiny helper rather than a strings.ToLower call
// inline — keeps the routing-key construction obvious in one place and
// gives us a single hook if normalisation ever needs to grow (e.g.,
// stripping whitespace, NFKC).
func lower(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
