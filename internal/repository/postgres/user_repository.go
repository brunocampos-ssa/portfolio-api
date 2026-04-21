package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// UserRepository implements contracts.UserRepository using PostgreSQL.
type UserRepository struct {
	db *sql.DB
}

// NewUserRepository creates a new PostgreSQL-backed user repository.
//
// Defensive programming: panic if db is nil. This is a programmer error —
// it means the application was wired incorrectly. Panicking at startup
// is acceptable because the program cannot function without a database.
func NewUserRepository(db *sql.DB) *UserRepository {
	if db == nil {
		panic("postgres.NewUserRepository: db must not be nil")
	}
	return &UserRepository{db: db}
}

// FindByID loads a user from the database by their unique ID.
//
// Error handling flow:
//   - sql.ErrNoRows → wrap with domain.ErrUserNotFound (business error)
//   - any other DB error → wrap with context using %w (infrastructure error)
//
// This distinction is critical: the service layer uses errors.Is to check
// for domain errors without knowing anything about SQL.
func (r *UserRepository) FindByID(ctx context.Context, id string) (*domain.User, error) {
	const op = "UserRepository.FindByID"

	query := `SELECT id, name, email, created_at FROM users WHERE id = $1`

	user := &domain.User{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			// Translate infrastructure error → domain error.
			// We wrap ErrUserNotFound so errors.Is(err, ErrUserNotFound) works.
			return nil, fmt.Errorf("%s: %w", op, domain.ErrUserNotFound)
		}
		// Unknown DB error — wrap with context but preserve the original.
		return nil, fmt.Errorf("%s: query failed: %w", op, err)
	}

	return user, nil
}
