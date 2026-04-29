// grpcclient — end-to-end demo of the gRPC authentication flow.
//
// What it does:
//   1. Register a fresh demo user via AuthService.Register (idempotent).
//   2. Login via AuthService.Login → access + refresh tokens.
//   3. Call PortfolioService.GetPortfolio with the access token in
//      gRPC "authorization" metadata.
//   4. Call WalletService.ListWallets the same way.
//
// Refresh and Logout are NOT mirrored on gRPC in Class 1 (per the trim
// decision) — refresh logistics live on the REST side. Aula 2 will
// pick them up alongside observability.
//
// Run:
//
//   make run                       # in another terminal
//   go run ./examples/grpcclient
//
//   GRPC_ADDR=staging.example.com:50051 go run ./examples/grpcclient
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/brunocampos-ssa/portfolio-api/gen/portfolio/v1"
)

const (
	demoEmail    = "alice-grpcclient@example.com"
	demoName     = "Alice (gRPC client demo)"
	demoPassword = "correct-horse-battery-staple"
)

func main() {
	addr := envOr("GRPC_ADDR", "localhost:50051")
	log.Printf("==> Dialling %s (insecure — add TLS for production)", addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	authClient := pb.NewAuthServiceClient(conn)
	portfolioClient := pb.NewPortfolioServiceClient(conn)
	walletClient := pb.NewWalletServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Register (idempotent — accept AlreadyExists).
	if _, err := authClient.Register(ctx, &pb.RegisterRequest{
		Name: demoName, Email: demoEmail, Password: demoPassword,
	}); err != nil {
		if !isAlreadyExists(err) {
			log.Fatalf("register: %v", err)
		}
		log.Printf("    user already exists, continuing")
	} else {
		log.Printf("    registered: %s", demoEmail)
	}

	// 2. Login.
	loginResp, err := authClient.Login(ctx, &pb.LoginRequest{
		Email: demoEmail, Password: demoPassword,
	})
	if err != nil {
		log.Fatalf("login: %v", err)
	}
	log.Printf("==> login OK")
	log.Printf("    access_token expires at  %s", loginResp.GetAccessTokenExpiresAt().AsTime().Format(time.RFC3339))
	log.Printf("    refresh_token expires at %s", loginResp.GetRefreshTokenExpiresAt().AsTime().Format(time.RFC3339))

	userID := jwtSubjectInsecure(loginResp.GetAccessToken())
	if userID == "" {
		log.Fatal("could not extract user id from access token")
	}
	log.Printf("    authenticated as user %s", userID)

	// 3. GetPortfolio with bearer token in metadata.
	authedCtx := withBearer(ctx, loginResp.GetAccessToken())

	portfolio, err := portfolioClient.GetPortfolio(authedCtx, &pb.GetPortfolioRequest{UserId: userID})
	if err != nil {
		log.Fatalf("GetPortfolio: %v", err)
	}
	log.Printf("==> GetPortfolio: total_usd=%.2f, holdings=%d",
		portfolio.GetTotalUsd(), len(portfolio.GetHoldings()))
	for _, h := range portfolio.GetHoldings() {
		if h.GetError() != "" {
			log.Printf("    [%s] %s: error=%q", h.GetBlockchain(), h.GetAddress(), h.GetError())
		} else {
			log.Printf("    [%s] %s: %f %s @ $%.2f = $%.2f",
				h.GetBlockchain(), h.GetAddress(),
				h.GetBalance(), h.GetAsset(), h.GetPriceUsd(), h.GetValueUsd())
		}
	}

	// 4. ListWallets.
	wallets, err := walletClient.ListWallets(authedCtx, &pb.ListWalletsRequest{UserId: userID})
	if err != nil {
		log.Fatalf("ListWallets: %v", err)
	}
	log.Printf("==> ListWallets: %d wallet(s)", len(wallets.GetWallets()))
	for _, w := range wallets.GetWallets() {
		log.Printf("    %s on %s: %s", w.GetId(), w.GetBlockchain(), w.GetAddress())
	}

	log.Println("Done. The same access_token works on the REST API too — try it!")
}

// =============================================================================
// helpers
// =============================================================================

// withBearer attaches "authorization: Bearer <token>" to the outgoing
// gRPC metadata. The interceptor on the server reads this exact header.
func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func isAlreadyExists(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code().String() == "AlreadyExists"
}

// jwtSubjectInsecure decodes the JWT payload and returns the "sub" claim
// without verifying the signature — the client trusts the server that
// just minted the token.
func jwtSubjectInsecure(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var c struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return ""
	}
	return c.Sub
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

