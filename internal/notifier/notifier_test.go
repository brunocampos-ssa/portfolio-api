package notifier_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	"github.com/brunocampos-ssa/portfolio-api/internal/notifier"
)

// TestHandler_InvokesNotifyWithEnvelope is the happy path: every
// delivered envelope reaches the notify callback unchanged.
func TestHandler_InvokesNotifyWithEnvelope(t *testing.T) {
	captured := make(chan *broker.EventEnvelope, 1)
	notify := func(_ context.Context, env *broker.EventEnvelope) error {
		captured <- env
		return nil
	}

	h := notifier.NewHandler(notify)
	in := newEnvelope("evt_1", "incoming", "USDC")
	require.NoError(t, h(context.Background(), in))

	got := <-captured
	require.Equal(t, in, got, "envelope must reach notify byte-identical")
}

// TestHandler_SurfacesNotifyErrorsForRetry verifies that notify
// failures bubble up so the consumer's bounded retry can absorb a
// transient destination outage (Slack down, email queue full, etc.).
// Swallowing here would silently lose alerts.
func TestHandler_SurfacesNotifyErrorsForRetry(t *testing.T) {
	notify := func(_ context.Context, _ *broker.EventEnvelope) error {
		return errors.New("slack 503")
	}

	h := notifier.NewHandler(notify)
	err := h(context.Background(), newEnvelope("evt_1", "incoming", "USDC"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "slack 503")
	require.Contains(t, err.Error(), "evt_1",
		"error message must include the event_id so operators can correlate alerts to deliveries")
}

// TestHandler_NilEnvelopeIsNoOp mirrors persister/router: nil
// envelopes commit (no-op) rather than tying up the consumer on a
// defensive programmer error.
func TestHandler_NilEnvelopeIsNoOp(t *testing.T) {
	var called atomic.Int32
	notify := func(_ context.Context, _ *broker.EventEnvelope) error {
		called.Add(1)
		return nil
	}

	h := notifier.NewHandler(notify)
	require.NoError(t, h(context.Background(), nil))
	require.Zero(t, called.Load(), "notify must NOT be called for a nil envelope")
}

// TestNewHandler_PanicsOnNilNotify follows the project-wide
// constructor pattern: fail at startup, not on the first message.
func TestNewHandler_PanicsOnNilNotify(t *testing.T) {
	require.Panics(t, func() { notifier.NewHandler(nil) })
}

func newEnvelope(eventID, direction, token string) *broker.EventEnvelope {
	return &broker.EventEnvelope{
		EventID:         eventID,
		SchemaVersion:   broker.SchemaCurrent,
		Network:         "ethereum",
		EventType:       "transfer",
		Direction:       direction,
		WalletID:        "w_1",
		TokenSymbol:     token,
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          "100.0",
		TxHash:          "0xdeadbeef",
		BlockNumber:     12345,
		EmittedAt:       time.Now().UTC(),
	}
}
