//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/stretchr/testify/require"
)


func TestNewUserRepository_NilDB(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when db is nil")
		}
	}()

	_ = postgres.NewUserRepository(nil)
}

func TestFindByID_UserNotFound(t *testing.T) {
	repo := postgres.NewUserRepository(database)

	_, err := repo.FindByID(context.Background(), "nonexistent-id")
	require.NotNil(t, err, "expected error when user not found")
	require.True(t, errors.Is(err, domain.ErrUserNotFound), "expected ErrUserNotFound, got %v", err)
}

func TestFindByID_DBError(t *testing.T) {
	// To simulate a DB error, we can create a repository with a closed database connection.
	db, err := sql.Open("postgres", "invalid-dsn")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.Close()

	repo := postgres.NewUserRepository(db)

	_, err = repo.FindByID(context.Background(), "any-id")
	require.NotNil(t, err, "expected error when database query fails")
	require.False(t, errors.Is(err, domain.ErrUserNotFound), "expected a DB error, not ErrUserNotFound")
}

func TestFindByID_Success(t *testing.T) {
	// This test assumes there is a user with ID "test-id" in the database.
	// In a real test, you would set up the database state before running this.
	repo := postgres.NewUserRepository(database)

	user, err := repo.FindByID(context.Background(), "u3")
	require.NoError(t, err, "unexpected error when finding user by ID")
	require.Equal(t, "u3", user.ID, "expected user ID 'u3', got '%s'", user.ID)
	require.Equal(t, "Multi Chain", user.Name, "expected user name 'Multi Chain', got '%s'", user.Name)
	require.Equal(t, "multi@example.com", user.Email, "expected user email 'multi@example.com', got '%s'", user.Email)
}