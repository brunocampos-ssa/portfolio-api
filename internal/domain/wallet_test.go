package domain_test

import (
	"errors"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

func TestValidateAddress(t *testing.T) {
	tests := []struct {
		name       string
		blockchain string
		address    string
		wantErr    error
	}{
		{
			name:       "valid ethereum address",
			blockchain: "ethereum",
			address:    "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
			wantErr:    nil,
		},
		{
			name:       "ethereum missing 0x prefix",
			blockchain: "ethereum",
			address:    "de0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
			wantErr:    domain.ErrInvalidWalletAddress,
		},
		{
			name:       "ethereum too short",
			blockchain: "ethereum",
			address:    "0xABC",
			wantErr:    domain.ErrInvalidWalletAddress,
		},
		{
			name:       "valid klever address",
			blockchain: "klever",
			address:    "klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8",
			wantErr:    nil,
		},
		{
			name:       "klever wrong prefix",
			blockchain: "klever",
			address:    "0xNotAKleverAddress",
			wantErr:    domain.ErrInvalidWalletAddress,
		},
		{
			name:       "empty address",
			blockchain: "ethereum",
			address:    "",
			wantErr:    domain.ErrInvalidWalletAddress,
		},
		{
			name:       "unknown blockchain passes basic check",
			blockchain: "solana",
			address:    "SomeAddress123",
			wantErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := domain.ValidateAddress(tt.blockchain, tt.address)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected errors.Is(%v, %v) = true", err, tt.wantErr)
			}
		})
	}
}
