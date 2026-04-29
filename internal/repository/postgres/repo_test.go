//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/postgres"
	_ "github.com/lib/pq"
)

var database *sql.DB

func TestMain(m *testing.M) {
	ctx := context.Background()

	pg, err := postgres.Start(ctx)
	if err != nil {
		log.Fatalf("FATAL: start PostgreSQL container: %v", err)
	}
	// Container teardown lives with the test binary exit — we want it to
	// run even when tests fail, which is why os.Exit is invoked at the end.
	defer func() { _ = pg.Stop(context.Background()) }()

	database, err = sql.Open("postgres", pg.DSN)
	if err != nil {
		log.Fatalf("FATAL: connect to PostgreSQL container: %v", err)
	}
	defer database.Close()

	if err := database.Ping(); err != nil {
		log.Fatalf("FATAL: ping PostgreSQL container: %v", err)
	}

	if err := postgres.RunMigrations(database); err != nil {
		log.Fatalf("FATAL: run migrations: %v", err)
	}

	// Run tests. Note: os.Exit skips deferred functions, so the cleanup
	// chain below relies on being the last thing we do before exit.
	code := m.Run()

	_ = database.Close()
	_ = pg.Stop(context.Background())
	os.Exit(code)
}
