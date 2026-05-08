package grpcapi

import (
	"context"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// clientUserAgent extracts the user-agent header from incoming gRPC metadata.
//
// gRPC clients populate this automatically (e.g. "grpc-go/1.74.2") so the
// audit trail in refresh_tokens reflects which client opened a session,
// the same way r.UserAgent() does on the HTTP side.
func clientUserAgent(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get("user-agent"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// clientIP returns the connecting peer's address (without port).
//
// Like its HTTP counterpart, this is recorded for audit ONLY — never
// used in an authorisation decision. Spoofing the peer addr would
// require gRPC TLS subject impersonation, which we are not doing.
func clientIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	addr := p.Addr.String()
	// Strip the trailing :port. IPv6 addrs need the bracket form, which
	// we leave intact here — the column is informational, not a primary
	// key.
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}
