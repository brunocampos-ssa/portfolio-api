package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

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

// FindByEmail loads a user by email, case-insensitive.
//
// The case-insensitive lookup matches the unique index added in migration
// 005 (CREATE UNIQUE INDEX ... ON users (lower(email))). Without lowering
// the input here too the index would help insertions but not lookups.
//
// Like FindByID, sql.ErrNoRows is translated to domain.ErrUserNotFound.
func (r *UserRepository) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	const op = "UserRepository.FindByEmail"

	query := `SELECT id, name, email, created_at, password_hash
	          FROM users WHERE lower(email) = lower($1)`

	user := &domain.User{}
	err := r.db.QueryRowContext(ctx, query, email).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.CreatedAt,
		&user.PasswordHash,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, domain.ErrUserNotFound)
		}
		return nil, fmt.Errorf("%s: query failed: %w", op, err)
	}
	return user, nil
}

// Create inserts a new user with the given password hash.
//
// Email collisions on the case-insensitive index surface as the standard
// Postgres unique_violation (SQLSTATE 23505). We translate that to
// domain.ErrUserAlreadyExists so the service layer can map it to a 409
// Conflict without inspecting SQL state.
func (r *UserRepository) Create(ctx context.Context, user *domain.User, passwordHash string) error {
	const op = "UserRepository.Create"

	if strings.TrimSpace(user.ID) == "" {
		return fmt.Errorf("%s: %w", op, domain.NewValidationError(op, "user id is required"))
	}

	query := `INSERT INTO users (id, name, email, password_hash, created_at)
	          VALUES ($1, $2, $3, $4, NOW())
	          RETURNING created_at`

	err := r.db.QueryRowContext(ctx, query,
		user.ID, user.Name, user.Email, passwordHash,
	).Scan(&user.CreatedAt)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return fmt.Errorf("%s: %w", op, domain.ErrUserAlreadyExists)
		}
		return fmt.Errorf("%s: insert failed: %w", op, err)
	}
	user.PasswordHash = passwordHash
	return nil
}
