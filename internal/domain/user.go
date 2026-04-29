package domain

import "time"

// User represents a registered user of the portfolio system.
//
// PasswordHash is excluded from JSON output via the `json:"-"` tag — it
// must never reach an API response. The repository layer reads it from
// `password_hash` and the auth service compares it against incoming
// passwords; nothing else should touch it.
type User struct {
	ID           string    `json:"id" db:"id"`
	Name         string    `json:"name" db:"name"`
	Email        string    `json:"email" db:"email"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	PasswordHash string    `json:"-" db:"password_hash"`
}
