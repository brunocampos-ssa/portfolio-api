package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/contracts"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/provider/blockchain"
)


// PortfolioService orchestrates the portfolio calculation flow.
// It depends only on interfaces (contracts), never on concrete implementations.
type PortfolioService struct {
	userRepo      contracts.UserRepository
	walletRepo    contracts.WalletRepository
	providerReg   *blockchain.ProviderRegistry
	priceProvider contracts.PriceProvider
}

// NewPortfolioService creates a service with all its dependencies injected.
//
// Defensive programming: we panic if any required dependency is nil.
// These are programmer errors — the application was wired incorrectly.
// Panicking at startup is better than a nil pointer dereference at runtime
// during a user request.
func NewPortfolioService(
	userRepo contracts.UserRepository,
	walletRepo contracts.WalletRepository,
	providerReg *blockchain.ProviderRegistry,
	priceProvider contracts.PriceProvider,
) *PortfolioService {
	if userRepo == nil {
		panic("service.NewPortfolioService: userRepo must not be nil")
	}
	if walletRepo == nil {
		panic("service.NewPortfolioService: walletRepo must not be nil")
	}
	if providerReg == nil {
		panic("service.NewPortfolioService: providerReg must not be nil")
	}
	if priceProvider == nil {
		panic("service.NewPortfolioService: priceProvider must not be nil")
	}
	return &PortfolioService{
		userRepo:      userRepo,
		walletRepo:    walletRepo,
		providerReg:   providerReg,
		priceProvider: priceProvider,
	}
}

// GetPortfolio executes the full portfolio calculation pipeline:
//  1. Load user from database
//  2. Load user's wallets
//  3. For each wallet: fetch balance + price, compute value
//  4. Aggregate into Portfolio
//
// Error translation happens here:
//   - Repository errors with domain.ErrUserNotFound → propagated as-is (already domain)
//   - Provider errors → wrapped into domain.AppError with appropriate code
//   - Individual wallet failures are isolated — one failing wallet doesn't kill the entire portfolio
func (s *PortfolioService) GetPortfolio(ctx context.Context, userID string) (*domain.Portfolio, error) {
	const op = "PortfolioService.GetPortfolio"

	// Step 1: Load user.
	// If errors.Is(err, domain.ErrUserNotFound), the handler maps to 404.
	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return nil, domain.NewNotFoundError(op, err)
		}
		return nil, domain.NewInternalError(op, err)
	}

	// Step 2: Load wallets.
	wallets, err := s.walletRepo.FindByUserID(ctx, userID)
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	if len(wallets) == 0 {
		return &domain.Portfolio{
			UserID:   user.ID,
			UserName: user.Name,
			Holdings: []domain.Holding{},
			TotalUSD: 0,
		}, nil
	}

	// Step 3: For each wallet, fetch balance and price.
	// Partial failure: if one wallet/provider fails, we record the error
	// in that holding and continue with the rest. This is a key resilience pattern.
	var holdings []domain.Holding
	var totalUSD float64

	for _, wallet := range wallets {
		holding := s.fetchHolding(ctx, wallet)
		totalUSD += holding.ValueUSD
		holdings = append(holdings, holding)
	}

	return &domain.Portfolio{
		UserID:   user.ID,
		UserName: user.Name,
		Holdings: holdings,
		TotalUSD: totalUSD,
	}, nil
}

// fetchHolding fetches balance and price for a single wallet.
// On failure, it returns a Holding with the Error field set rather than
// failing the entire portfolio request.
//
// This demonstrates:
//   - defer for timing/logging
//   - error classification (timeout vs unavailable vs other)
//   - graceful degradation
func (s *PortfolioService) fetchHolding(ctx context.Context, wallet domain.Wallet) domain.Holding {
	// defer for elapsed-time logging — runs when the function returns.
	start := time.Now()
	defer func() {
		log.Printf("fetchHolding: wallet=%s blockchain=%s elapsed=%s",
			wallet.Address[:min(8, len(wallet.Address))], wallet.Blockchain, time.Since(start))
	}()

	holding := domain.Holding{
		Blockchain: wallet.Blockchain,
		Address:    wallet.Address,
	}

	// Resolve provider for this wallet's blockchain.
	provider, err := s.providerReg.Get(wallet.Blockchain)
	if err != nil {
		holding.Error = "unsupported blockchain: " + wallet.Blockchain
		return holding
	}

	// Fetch on-chain balance.
	balance, asset, err := provider.GetBalance(ctx, wallet.Address)
	if err != nil {
		// Classify the error for the holding's error message.
		switch {
		case errors.Is(err, domain.ErrUpstreamTimeout):
			holding.Error = "balance fetch timed out"
		case errors.Is(err, domain.ErrProviderUnavailable):
			holding.Error = "balance provider unavailable"
		default:
			holding.Error = "failed to fetch balance"
		}
		log.Printf("ERROR: fetchHolding balance: %v", err)
		return holding
	}

	holding.Asset = asset
	holding.Balance = balance

	// Fetch price.
	priceUSD, err := s.priceProvider.GetPriceUSD(ctx, asset)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrUpstreamTimeout):
			holding.Error = "price fetch timed out"
		case errors.Is(err, domain.ErrProviderUnavailable):
			holding.Error = "price provider unavailable"
		case errors.Is(err, domain.ErrUnsupportedAsset):
			holding.Error = "unsupported asset: " + asset
		default:
			holding.Error = "failed to fetch price"
		}
		log.Printf("ERROR: fetchHolding price: %v", err)
		return holding
	}

	holding.PriceUSD = priceUSD
	holding.ValueUSD = balance * priceUSD

	return holding
}

// GetWallets returns all wallets for a user.
func (s *PortfolioService) GetWallets(ctx context.Context, userID string) ([]domain.Wallet, error) {
	const op = "PortfolioService.GetWallets"

	// Verify user exists first.
	_, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return nil, domain.NewNotFoundError(op, err)
		}
		return nil, domain.NewInternalError(op, err)
	}

	wallets, err := s.walletRepo.FindByUserID(ctx, userID)
	if err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	return wallets, nil
}

// AddWallet validates and creates a new wallet for a user.
func (s *PortfolioService) AddWallet(ctx context.Context, userID, blockchain, address string) (*domain.Wallet, error) {
	const op = "PortfolioService.AddWallet"

	// Verify user exists.
	_, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return nil, domain.NewNotFoundError(op, err)
		}
		return nil, domain.NewInternalError(op, err)
	}

	// Validate address format.
	if err := domain.ValidateAddress(blockchain, address); err != nil {
		return nil, &domain.AppError{
			Code:    domain.CodeValidation,
			Message: "invalid wallet address for " + blockchain,
			Op:      op,
			Err:     err,
		}
	}

	// Verify we support this blockchain.
	if _, err := s.providerReg.Get(blockchain); err != nil {
		return nil, &domain.AppError{
			Code:    domain.CodeUnsupportedNetwork,
			Message: "unsupported blockchain: " + blockchain,
			Op:      op,
			Err:     err,
		}
	}

	wallet := &domain.Wallet{
		ID:         generateID(),
		UserID:     userID,
		Blockchain: blockchain,
		Address:    address,
	}

	if err := s.walletRepo.Create(ctx, wallet); err != nil {
		return nil, domain.NewInternalError(op, err)
	}

	return wallet, nil
}

// generateID creates a simple unique ID. In production, use UUID.
func generateID() string {
	return fmt.Sprintf("w%d", time.Now().UnixNano())
}
