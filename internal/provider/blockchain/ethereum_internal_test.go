package blockchain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// Whitebox tests for the small pure helpers that back the Ethereum provider.
// Keeping them in the same package lets us exercise weiToETH / padAddress
// without exporting them purely for testing.
// =============================================================================

func TestWeiToETH(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want float64
	}{
		{"zero", "0x0", 0},
		{"1 eth", "0xde0b6b3a7640000", 1},
		{"0.5 eth", "0x6f05b59d3b20000", 0.5},
		{"with prefix stripped", "de0b6b3a7640000", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := weiToETH(tt.hex)
			require.NoError(t, err)
			require.InDelta(t, tt.want, got, 1e-12)
		})
	}
}

func TestWeiToETH_Invalid(t *testing.T) {
	_, err := weiToETH("0xzz")
	require.Error(t, err)
}

func TestPadAddress(t *testing.T) {
	got := padAddress("0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe")
	want := "0x000000000000000000000000de0b295669a9fd93d5f28d9ec85e40f4cb697bae"
	require.Equal(t, want, got)
}

// TestEthereumProvider_GetBalance_HTTPFlow verifies the happy path end-to-end
// with an httptest server standing in for an Ethereum node. This exercises
// JSON envelope handling, hex conversion, and error wrapping.
func TestEthereumProvider_GetBalance_HTTPFlow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req struct {
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		require.NoError(t, json.Unmarshal(body, &req))
		require.Equal(t, "eth_getBalance", req.Method)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xde0b6b3a7640000"}`))
	}))
	defer srv.Close()

	p := NewEthereumProvider(srv.URL)
	balance, asset, err := p.GetBalance(t.Context(), "0xAAA")

	require.NoError(t, err)
	require.Equal(t, "ETH", asset)
	require.InDelta(t, 1.0, balance, 1e-12)
}

// TestEthereumProvider_GetBalance_UpstreamError verifies that non-200 status
// codes are wrapped with the ErrProviderUnavailable sentinel.
func TestEthereumProvider_GetBalance_UpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream fault"}`))
	}))
	defer srv.Close()

	p := NewEthereumProvider(srv.URL)
	_, _, err := p.GetBalance(t.Context(), "0xAAA")
	require.Error(t, err)
	require.True(t, errors.Is(err, domain.ErrProviderUnavailable))
}

// TestEthereumProvider_GetBalance_RPCError exercises the JSON-RPC error envelope.
func TestEthereumProvider_GetBalance_RPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}`))
	}))
	defer srv.Close()

	p := NewEthereumProvider(srv.URL)
	_, _, err := p.GetBalance(t.Context(), "0xAAA")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid params")
}

// TestEthereumTokenProvider_GetTokenBalance_HTTPFlow covers the ERC-20 provider.
// The request body encodes the balanceOf(address) selector — spot-check it.
func TestEthereumTokenProvider_GetTokenBalance_HTTPFlow(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		// balance = 1_000_000 (raw); decimals=6 → 1.0
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x00000000000000000000000000000000000000000000000000000000000f4240"}`))
	}))
	defer srv.Close()

	p := NewEthereumTokenProvider(srv.URL)
	bal, err := p.GetTokenBalance(t.Context(), "0xAAA", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6)
	require.NoError(t, err)
	require.InDelta(t, 1.0, bal, 1e-12)

	// balanceOf selector present.
	require.True(t, bytes.Contains(captured, []byte("0x70a08231")),
		"expected balanceOf selector in call data")
}

// =============================================================================
// Benchmark — tests the parsing hot path.
// =============================================================================

func BenchmarkWeiToETH(b *testing.B) {
	const hex = "0xde0b6b3a7640000" // 1 ETH in wei
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = weiToETH(hex)
	}
}

func BenchmarkPadAddress(b *testing.B) {
	const addr = "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = padAddress(addr)
	}
}
