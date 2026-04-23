package testenv

import (
	"context"
	"fmt"
	"time"
)

// =============================================================================
// DB seed helpers.
//
// Most of the DB seed data is driven by migration 002_seed_data.up.sql, which
// runs automatically as part of postgres.RunMigrations during Setup. The
// helpers here exist for tests that need to append an additional user or
// wallet on top of the baseline seed.
//
// All inserts use ON CONFLICT DO NOTHING so tests can be rerun against the
// same environment without teardown between runs.
// =============================================================================

// SeedUserAndWallet inserts a user + a single tracked ETH wallet. Safe to
// call multiple times with the same IDs.
func (e *Env) SeedUserAndWallet(ctx context.Context, walletID, userID, name, addr string) error {
	if e == nil || e.DB == nil {
		return fmt.Errorf("testenv: environment not initialised")
	}
	insertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := e.DB.ExecContext(insertCtx,
		`INSERT INTO users (id, name, email)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO NOTHING`,
		userID, name, userID+"@example.com",
	); err != nil {
		return fmt.Errorf("testenv.SeedUserAndWallet: user: %w", err)
	}
	if _, err := e.DB.ExecContext(insertCtx,
		`INSERT INTO wallets (id, user_id, blockchain, address)
		 VALUES ($1, $2, 'ethereum', $3)
		 ON CONFLICT (id) DO NOTHING`,
		walletID, userID, addr,
	); err != nil {
		return fmt.Errorf("testenv.SeedUserAndWallet: wallet: %w", err)
	}
	return nil
}

// GetTrackedETHAddresses returns the distinct lowercased Ethereum wallet
// addresses currently in the DB. Bootstrap uses this to know which wallets
// to seed on the forked chain; tests can use it to discover what's already
// seeded without hardcoding addresses.
func (e *Env) GetTrackedETHAddresses(ctx context.Context) ([]string, error) {
	if e == nil || e.DB == nil {
		return nil, fmt.Errorf("testenv: environment not initialised")
	}
	rows, err := e.DB.QueryContext(ctx,
		`SELECT DISTINCT LOWER(address)
		 FROM wallets
		 WHERE blockchain = 'ethereum'
		 ORDER BY LOWER(address)`,
	)
	if err != nil {
		return nil, fmt.Errorf("testenv.GetTrackedETHAddresses: query: %w", err)
	}
	defer rows.Close()

	var addrs []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, fmt.Errorf("testenv.GetTrackedETHAddresses: scan: %w", err)
		}
		addrs = append(addrs, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("testenv.GetTrackedETHAddresses: rows: %w", err)
	}
	return addrs, nil
}
