package persister_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/brunocampos-ssa/portfolio-api/internal/broker"
	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
	"github.com/brunocampos-ssa/portfolio-api/internal/persister"
	"github.com/brunocampos-ssa/portfolio-api/internal/testutil/mocks"
)

// TestHandler_PersistsEnvelope_AsIs is the happy-path: a well-formed
// envelope reaches the repo with the right fields. Critically, the
// row's id MUST be the envelope's EventID — that's the contract that
// makes ON CONFLICT (id) DO NOTHING the idempotency mechanism.
func TestHandler_PersistsEnvelope_AsIs(t *testing.T) {
	repo := &mocks.EventRepository{}
	captured := make(chan *domain.WalletEvent, 1)
	repo.
		On("Create", mock.Anything, mock.AnythingOfType("*domain.WalletEvent")).
		Run(func(args mock.Arguments) {
			captured <- args.Get(1).(*domain.WalletEvent)
		}).
		Return(nil)

	h := persister.NewHandler(repo)
	env := newEnvelope("evt_1", "ethereum", "incoming")

	require.NoError(t, h(context.Background(), env))

	got := <-captured
	require.Equal(t, env.EventID, got.ID, "row id must equal envelope event_id (idempotency key)")
	require.Equal(t, env.WalletID, got.WalletID)
	require.Equal(t, env.TxHash, got.TxHash)
	require.Equal(t, env.BlockNumber, got.BlockNumber)
	require.Equal(t, env.ContractAddress, got.ContractAddress)
	require.Equal(t, env.EventType, got.EventType)
	require.Equal(t, env.Direction, got.Direction)
	require.Equal(t, env.Amount, got.Amount)
	require.Equal(t, env.TokenSymbol, got.TokenSymbol)
	repo.AssertExpectations(t)
}

// TestHandler_RetriesOnRepoError verifies the handler bubbles up
// transient errors so the kafka.Consumer's bounded retry can absorb
// them. The next attempt MUST go through the repo again — we don't
// short-circuit on previous-attempt-failed.
func TestHandler_RetriesOnRepoError(t *testing.T) {
	repo := &mocks.EventRepository{}
	repo.
		On("Create", mock.Anything, mock.Anything).
		Return(errors.New("connection reset by peer")).
		Once()

	h := persister.NewHandler(repo)
	err := h(context.Background(), newEnvelope("evt_1", "ethereum", "incoming"))
	require.Error(t, err, "transient repo errors must surface so consumer retries")
	require.Contains(t, err.Error(), "connection reset")
	repo.AssertExpectations(t)
}

// TestHandler_DropsInvalidEnvelopes verifies that envelopes with
// unrecoverable validation errors do NOT consume retry budget. The
// handler logs the drop and returns nil so the consumer commits the
// offset immediately and moves on. The repo is never called.
//
// Why nil and not an error? Because retrying a malformed payload
// will produce the same failure deterministically — burning 3 retries
// per bad message delays every following message in the partition.
// The log line is the operator-visible signal of the drop.
func TestHandler_DropsInvalidEnvelopes(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*broker.EventEnvelope)
	}{
		{
			name: "bad direction",
			mut:  func(e *broker.EventEnvelope) { e.Direction = "sideways" },
		},
		{
			name: "empty event_type",
			mut:  func(e *broker.EventEnvelope) { e.EventType = "" },
		},
		{
			name: "empty token_symbol",
			mut:  func(e *broker.EventEnvelope) { e.TokenSymbol = "" },
		},
		{
			name: "empty amount",
			mut:  func(e *broker.EventEnvelope) { e.Amount = "" },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mocks.EventRepository{}
			h := persister.NewHandler(repo)
			env := newEnvelope("evt_1", "ethereum", "incoming")
			tc.mut(env)

			err := h(context.Background(), env)
			require.NoError(t, err,
				"validation drops must return nil so the consumer commits without retrying")
			repo.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
		})
	}
}

// TestHandler_NilEnvelopeIsNoOp guards against the kafka.Consumer or
// any future caller invoking with a nil envelope. We commit the
// offset (return nil) rather than tying up the consumer on a
// programmer error.
func TestHandler_NilEnvelopeIsNoOp(t *testing.T) {
	repo := &mocks.EventRepository{}
	h := persister.NewHandler(repo)
	require.NoError(t, h(context.Background(), nil))
	repo.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
}

// TestNewHandler_PanicsOnNilRepo mirrors the constructor pattern used
// across the codebase — fail at startup, not on the first message.
func TestNewHandler_PanicsOnNilRepo(t *testing.T) {
	require.Panics(t, func() { persister.NewHandler(nil) })
}

func newEnvelope(eventID, network, direction string) *broker.EventEnvelope {
	return &broker.EventEnvelope{
		EventID:         eventID,
		SchemaVersion:   broker.SchemaCurrent,
		Network:         network,
		EventType:       "transfer",
		Direction:       direction,
		WalletID:        "w_1",
		TokenSymbol:     "USDC",
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          "100.0",
		TxHash:          "0xdeadbeef",
		BlockNumber:     12345,
		EmittedAt:       time.Now().UTC(),
	}
}
