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
	require.NoError(t, err, "unexpected error when finding wallets for user with wallets")
	require.Len(t, wallets, 2, "expected exactly 2 wallets for user 'u3'")

	addresses := make([]string, 0, len(wallets))
	for _, wallet := range wallets {
		addresses = append(addresses, wallet.Address)
	}

	require.ElementsMatch(
		t,
		[]string{
			"0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
			"klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8",
		},
		addresses,
		"expected wallet addresses for user 'u3' did not match",
	)
}