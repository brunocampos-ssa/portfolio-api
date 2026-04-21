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
	databaseURL, err := postgres.GetPostgresContainer(context.Background())
	if err != nil {
		log.Fatalf("FATAL: start PostgreSQL container: %v", err)
	}

	database, err = sql.Open("postgres", databaseURL)
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

	// Run tests.
	code := m.Run()

	// Exit with the test code.
	os.Exit(code)
}