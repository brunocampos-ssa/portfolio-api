// Package rabbittest boots a single-node RabbitMQ container for
// integration tests. Mirrors internal/testutil/kafkatest in shape:
// Start returns a Handle, callers Stop it on cleanup.
//
// We delegate to testcontainers-go/modules/rabbitmq because its
// AmqpURL helper handles the host/port/credential dance for us.
// docker-compose.yml uses the same upstream image (rabbitmq:3-management)
// so what students see locally and what the test suite sees match.
package rabbittest

import (
	"context"
	"fmt"

	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// Handle wraps a running RabbitMQ container and the AMQP URL host
// clients should connect to. Callers MUST Stop the handle on cleanup
// or containers leak across `go test` runs.
type Handle struct {
	container *tcrabbit.RabbitMQContainer

	// URL is the AMQP connection string in
	// amqp://user:pass@host:port/ form, ready to pass to
	// rabbitmq.NewPublisher / NewConsumer.
	URL string
}

// Stop terminates the underlying container. Idempotent and safe on a
// nil Handle.
func (h *Handle) Stop(ctx context.Context) error {
	if h == nil || h.container == nil {
		return nil
	}
	return h.container.Terminate(ctx)
}

// Start boots a fresh RabbitMQ container with the management plugin.
// Default credentials guest/guest, matching docker-compose.yml.
func Start(ctx context.Context) (*Handle, error) {
	c, err := tcrabbit.Run(ctx,
		"rabbitmq:3.13-management",
		tcrabbit.WithAdminUsername("guest"),
		tcrabbit.WithAdminPassword("guest"),
	)
	if err != nil {
		return nil, fmt.Errorf("rabbittest.Start: run container: %w", err)
	}

	url, err := c.AmqpURL(ctx)
	if err != nil {
		_ = c.Terminate(context.Background())
		return nil, fmt.Errorf("rabbittest.Start: amqp url: %w", err)
	}

	return &Handle{container: c, URL: url}, nil
}
