package domain

import (
	"strings"
	"time"
)

// Wallet represents a blockchain wallet associated with a user.
type Wallet struct {
	ID         string    `json:"id" db:"id"`
	UserID     string    `json:"user_id" db:"user_id"`
	Blockchain string    `json:"blockchain" db:"blockchain"`
	Address    string    `json:"address" db:"address"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// ValidateAddress performs basic address validation per blockchain.
// Returns ErrInvalidWalletAddress if the address is clearly malformed.
//
// This is defensive programming: validate input at the boundary
// before it propagates deeper into the system.
func ValidateAddress(blockchain, address string) error {
	if address == "" {
		return ErrInvalidWalletAddress
	}

	switch strings.ToLower(blockchain) {
	case "ethereum":
		if !strings.HasPrefix(address, "0x") || len(address) != 42 {
			return ErrInvalidWalletAddress
		}
	case "klever":
		if !strings.HasPrefix(address, "klv1") {
			return ErrInvalidWalletAddress
		}
	}

	return nil
}
