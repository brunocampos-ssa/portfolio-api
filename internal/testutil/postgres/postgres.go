package postgres

import (
	"context"
	"fmt"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// dbPort is the Postgres public TCP port, declared in "port/proto" form so
// every testcontainers-go API sees the same value.
const dbPort = "5432/tcp"

// Handle wraps a running Postgres testcontainer and the DSN required to
// connect to it. Callers MUST call Stop to terminate the container —
// leaving Handles un-stopped leaks containers across `go test` runs.
type Handle struct {
	container tc.Container
	DSN       string
}

// Stop terminates the underlying container. Safe to call on a nil Handle
// or one whose container was never created.
func (h *Handle) Stop(ctx context.Context) error {
	if h == nil || h.container == nil {
		return nil
	}
	return h.container.Terminate(ctx)
}

// Start boots a fresh Postgres 15-alpine testcontainer and returns a
// *Handle that exposes the DSN and a teardown method.
func Start(ctx context.Context) (*Handle, error) {
	req := tc.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{dbPort},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections"),
			wait.ForListeningPort(dbPort),
		).WithDeadline(30 * time.Second),
	}

	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("postgres.Start: create container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("postgres.Start: host: %w", err)
	}

	// dbPort is declared in "port/proto" form so every testcontainers-go
	// API (ExposedPorts, ForListeningPort, MappedPort) sees the same value.
	port, err := container.MappedPort(ctx, dbPort)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("postgres.Start: mapped port: %w", err)
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable",
		host, port.Port())

	return &Handle{container: container, DSN: dsn}, nil
}
