package router_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	"github.com/brunocampos-ssa/portfolio-api/internal/router"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/mocks"
)

// TestHandler_ForwardsEnvelopeAsIs verifies the happy path: the
// router passes the envelope to the publisher unchanged. The bridge
// is deliberately lossless — no transformations, no dropping of
// fields, no policy decisions. That's what makes it composable.
func TestHandler_ForwardsEnvelopeAsIs(t *testing.T) {
	pub := &mocks.BrokerPublisher{}
	captured := make(chan *broker.EventEnvelope, 1)
	pub.
		On("Publish", mock.Anything, mock.AnythingOfType("*broker.EventEnvelope")).
		Run(func(args mock.Arguments) {
			captured <- args.Get(1).(*broker.EventEnvelope)
		}).
		Return(nil)

	h := router.NewHandler(pub)
	in := newEnvelope("evt_1", "incoming", "USDC")
	require.NoError(t, h(context.Background(), in))

	got := <-captured
	require.Equal(t, in, got, "envelope must be forwarded byte-identical")
	pub.AssertExpectations(t)
}

// TestHandler_SurfacesPublishErrorsForRetry verifies that downstream
// publish failures surface as errors so the kafka.Consumer's bounded
// retry can absorb them. Returning nil here would silently lose the
// event — the Kafka offset would commit while RabbitMQ never received
// the message.
func TestHandler_SurfacesPublishErrorsForRetry(t *testing.T) {
	pub := &mocks.BrokerPublisher{}
	pub.
		On("Publish", mock.Anything, mock.Anything).
		Return(errors.New("rabbitmq broker NACK")).
		Once()

	h := router.NewHandler(pub)
	err := h(context.Background(), newEnvelope("evt_1", "incoming", "USDC"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "rabbitmq broker NACK",
		"publisher error must be wrapped, not swallowed")
	pub.AssertExpectations(t)
}

// TestHandler_NilEnvelopeIsNoOp matches the persister's contract:
// nil envelopes commit (no-op) rather than tying up the consumer on
// a defensive programmer error.
func TestHandler_NilEnvelopeIsNoOp(t *testing.T) {
	pub := &mocks.BrokerPublisher{}
	h := router.NewHandler(pub)
	require.NoError(t, h(context.Background(), nil))
	pub.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything)
}

// TestNewHandler_PanicsOnNilPublisher mirrors the project-wide
// pattern: catch wiring mistakes at startup, not on the first
// message.
func TestNewHandler_PanicsOnNilPublisher(t *testing.T) {
	require.Panics(t, func() { router.NewHandler(nil) })
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
