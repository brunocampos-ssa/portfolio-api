//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/brunocampos-ssa/portfolio-api/internal/repository/postgres"
	"github.com/stretchr/testify/require"
)

func TestNewWalletRepository_NilDB(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when db is nil")
		}
	}()
	_ = postgres.NewWalletRepository(nil)
}

func TestFindByUserID_UserHasWallets(t *testing.T) {
	repo := postgres.NewWalletRepository(database)

	wallets, err := repo.FindByUserID(context.Background(), "u3")
	require.NoError(t, err, "unexpected error when finding wallets for user with no wallets")
	require.NotEmpty(t, wallets, "expected at least one wallet for user 'u3'")
	require.Equal(t, "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe", wallets[0].Address, "expected wallet address '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', got '%s'", wallets[0].Address)
	require.Equal(t, "klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8", wallets[1].Address, "expected wallet address 'klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8', got '%s'", wallets[1].Address)
}