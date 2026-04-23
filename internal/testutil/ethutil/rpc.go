// Package ethutil is a tiny JSON-RPC client used by integration tests to talk
// to Anvil. It is intentionally dependency-free (no go-ethereum) so this test
// helper stays small and the module graph does not balloon for students.
//
// Anvil implements a superset of the Ethereum JSON-RPC API plus custom
// methods for test-time state mutation (`anvil_impersonateAccount`,
// `anvil_setBalance`, `anvil_setCode`, `anvil_setStorageAt`). Those are the
// ones we wrap here.
package ethutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Client is a minimal JSON-RPC 2.0 client. It is goroutine-safe: a single
// Client can be shared by multiple tests in parallel.
type Client struct {
	url  string
	http *http.Client
	id   atomic.Int64
}

// NewClient builds a Client pointing at the given JSON-RPC endpoint.
// A zero-value timeout means "use a sensible default" (10 seconds).
func NewClient(url string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{
		url:  url,
		http: &http.Client{Timeout: timeout},
	}
}

// rpcRequest matches the JSON-RPC 2.0 envelope.
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int64  `json:"id"`
}

// rpcResponse is a generic envelope; Result is decoded into the caller's
// destination with a second json.Unmarshal pass.
type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	ID     int64           `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// Call performs a JSON-RPC call and decodes the result into `out`.
// If `out` is nil, the result is discarded (useful for methods that return `null`).
func (c *Client) Call(ctx context.Context, method string, params []any, out any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      c.id.Add(1),
	})
	if err != nil {
		return fmt.Errorf("ethutil: marshal %s: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ethutil: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ethutil: %s: %w", method, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ethutil: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ethutil: %s: HTTP %d: %s", method, resp.StatusCode, string(data))
	}

	var envelope rpcResponse
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("ethutil: decode envelope: %w (body=%s)", err, string(data))
	}
	if envelope.Error != nil {
		return fmt.Errorf("ethutil: %s: %w", method, envelope.Error)
	}
	if out == nil || len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("ethutil: decode result: %w (result=%s)", err, string(envelope.Result))
	}
	return nil
}

// URL returns the JSON-RPC endpoint this client is pointed at.
// Handy when tests need to hand the URL to production code paths
// (e.g. NewEthereumProvider).
func (c *Client) URL() string { return c.url }
