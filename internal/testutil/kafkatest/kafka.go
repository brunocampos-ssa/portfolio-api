// Package kafkatest boots a single-node Kafka container in KRaft mode
// for integration tests. Mirrors internal/testutil/postgres in shape:
// Start returns a Handle, callers Stop it on cleanup.
//
// We delegate to testcontainers-go/modules/kafka rather than rolling
// our own GenericContainer because the "tell the broker the host port
// the host clients will use" dance (rewrite ADVERTISED_LISTENERS after
// the random port is known) is genuinely fiddly to get right and the
// module already handles it. The postgres helper hand-rolls because
// Postgres's bootstrap is simpler — no advertised-listener equivalent.
//
// The module defaults to confluentinc/confluent-local, which is a
// minimal Kafka-in-KRaft-mode image maintained by Confluent. That's a
// different image from the one in docker-compose.yml (apache/kafka),
// but both speak vanilla Kafka at the wire level — for testing what we
// care about is wire-protocol fidelity, not entrypoint-script
// matching. Local dev gets the canonical Apache image; tests get the
// hassle-free Confluent one.
package kafkatest

import (
	"context"
	"fmt"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Handle wraps a running Kafka container and the bootstrap address(es)
// host clients should connect to. Callers MUST Stop the handle on
// cleanup or containers leak across `go test` runs.
type Handle struct {
	container *tckafka.KafkaContainer

	// Brokers is the bootstrap server list in host:port form, ready to
	// pass to kafka.NewPublisher / NewConsumer.
	Brokers []string
}

// Stop terminates the underlying container. Idempotent and safe on a
// nil Handle.
func (h *Handle) Stop(ctx context.Context) error {
	if h == nil || h.container == nil {
		return nil
	}
	return h.container.Terminate(ctx)
}

// Start boots a fresh Kafka KRaft container. Returns a Handle exposing
// the broker bootstrap address.
func Start(ctx context.Context) (*Handle, error) {
	c, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("portfolio-test"),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkatest.Start: run container: %w", err)
	}

	brokers, err := c.Brokers(ctx)
	if err != nil {
		_ = c.Terminate(context.Background())
		return nil, fmt.Errorf("kafkatest.Start: brokers: %w", err)
	}

	return &Handle{container: c, Brokers: brokers}, nil
}
