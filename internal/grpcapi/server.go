package grpcapi

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/brunocampos-ssa/portfolio-api/gen/portfolio/v1"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/service"
)

// =============================================================================
// gRPC server implementations
// =============================================================================
//
// One file, three services. Each server type implements the *Server
// interface generated under gen/portfolio/v1 and delegates to the same
// domain services the REST handlers call. The transport layer is thin
// by design: nothing here knows about argon2, JWT signing, or SQL —
// the policy lives in service/.
//
// Mapping idiom (per RPC):
//   1. Authorization (requireSelf) for protected RPCs.
//   2. Adapt the protobuf request → service input struct.
//   3. Call the service.
//   4. Translate errors via toStatus, results back to protobuf.

// =============================================================================
// AuthService — gRPC façade for service.AuthService
// =============================================================================
//
// Class 1 ships only Register and Login on the gRPC surface — Refresh
// and Logout stay REST-only (see README, Aula 2 hooks).

type AuthServer struct {
	pb.UnimplementedAuthServiceServer
	svc *service.AuthService
}

func NewAuthServer(svc *service.AuthService) *AuthServer {
	if svc == nil {
		panic("grpcapi.NewAuthServer: svc must not be nil")
	}
	return &AuthServer{svc: svc}
}

func (s *AuthServer) Register(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	user, err := s.svc.Register(ctx, service.RegisterInput{
		Name:     in.GetName(),
		Email:    in.GetEmail(),
		Password: in.GetPassword(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.RegisterResponse{
		Id:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: timestamppb.New(user.CreatedAt),
	}, nil
}

func (s *AuthServer) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error) {
	tokens, err := s.svc.Login(ctx, service.LoginInput{
		Email:     in.GetEmail(),
		Password:  in.GetPassword(),
		UserAgent: clientUserAgent(ctx),
		IP:        clientIP(ctx),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.LoginResponse{
		AccessToken:           tokens.AccessToken,
		AccessTokenExpiresAt:  timestamppb.New(tokens.AccessTokenExpiresAt),
		RefreshToken:          tokens.RefreshToken,
		RefreshTokenExpiresAt: timestamppb.New(tokens.RefreshTokenExpiresAt),
	}, nil
}

// =============================================================================
// PortfolioService
// =============================================================================

type PortfolioServer struct {
	pb.UnimplementedPortfolioServiceServer
	svc *service.PortfolioService
}

func NewPortfolioServer(svc *service.PortfolioService) *PortfolioServer {
	if svc == nil {
		panic("grpcapi.NewPortfolioServer: svc must not be nil")
	}
	return &PortfolioServer{svc: svc}
}

func (s *PortfolioServer) GetPortfolio(ctx context.Context, in *pb.GetPortfolioRequest) (*pb.GetPortfolioResponse, error) {
	if err := requireSelf(ctx, in.GetUserId()); err != nil {
		return nil, err
	}
	p, err := s.svc.GetPortfolio(ctx, in.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return portfolioToProto(p), nil
}

// =============================================================================
// WalletService
// =============================================================================

type WalletServer struct {
	pb.UnimplementedWalletServiceServer
	svc *service.PortfolioService
}

func NewWalletServer(svc *service.PortfolioService) *WalletServer {
	if svc == nil {
		panic("grpcapi.NewWalletServer: svc must not be nil")
	}
	return &WalletServer{svc: svc}
}

func (s *WalletServer) ListWallets(ctx context.Context, in *pb.ListWalletsRequest) (*pb.ListWalletsResponse, error) {
	if err := requireSelf(ctx, in.GetUserId()); err != nil {
		return nil, err
	}
	wallets, err := s.svc.GetWallets(ctx, in.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*pb.Wallet, 0, len(wallets))
	for _, w := range wallets {
		out = append(out, walletToProto(&w))
	}
	return &pb.ListWalletsResponse{Wallets: out}, nil
}

func (s *WalletServer) AddWallet(ctx context.Context, in *pb.AddWalletRequest) (*pb.AddWalletResponse, error) {
	if err := requireSelf(ctx, in.GetUserId()); err != nil {
		return nil, err
	}
	wallet, err := s.svc.AddWallet(ctx, in.GetUserId(), in.GetBlockchain(), in.GetAddress())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.AddWalletResponse{Wallet: walletToProto(wallet)}, nil
}

// =============================================================================
// adapters: domain ↔ proto
// =============================================================================

func portfolioToProto(p *domain.Portfolio) *pb.GetPortfolioResponse {
	holdings := make([]*pb.Holding, 0, len(p.Holdings))
	for _, h := range p.Holdings {
		holdings = append(holdings, &pb.Holding{
			Blockchain: h.Blockchain,
			Address:    h.Address,
			Asset:      h.Asset,
			Balance:    h.Balance,
			PriceUsd:   h.PriceUSD,
			ValueUsd:   h.ValueUSD,
			Error:      h.Error,
		})
	}
	return &pb.GetPortfolioResponse{
		UserId:   p.UserID,
		UserName: p.UserName,
		Holdings: holdings,
		TotalUsd: p.TotalUSD,
	}
}

func walletToProto(w *domain.Wallet) *pb.Wallet {
	return &pb.Wallet{
		Id:         w.ID,
		UserId:     w.UserID,
		Blockchain: w.Blockchain,
		Address:    w.Address,
		CreatedAt:  timestamppb.New(w.CreatedAt),
	}
}
