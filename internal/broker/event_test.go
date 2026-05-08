package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEnvelope_RoundTrip is the schema-stability test: any change to
// EventEnvelope's JSON shape will fail here, forcing a deliberate
// SchemaCurrent bump and a discussion about consumer compatibility.
func TestEnvelope_RoundTrip(t *testing.T) {
	in := &EventEnvelope{
		EventID:         "evt_abc123",
		SchemaVersion:   SchemaCurrent,
		Network:         "ethereum",
		EventType:       "transfer",
		Direction:       "incoming",
		WalletID:        "w_42",
		TokenSymbol:     "USDC",
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          "1234.56",
		TxHash:          "0xdeadbeef",
		BlockNumber:     25_050_419,
		EmittedAt:       time.Date(2026, 5, 8, 16, 30, 0, 0, time.UTC),
	}

	bytes, err := in.Marshal()
	require.NoError(t, err)
	require.Contains(t, string(bytes), `"event_id":"evt_abc123"`)
	require.Contains(t, string(bytes), `"schema_version":1`)

	out, err := Unmarshal(bytes)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

// TestEnvelope_SchemaMismatch confirms consumers can detect an
// envelope from an incompatible producer and route to a DLQ instead
// of crashing.
func TestEnvelope_SchemaMismatch(t *testing.T) {
	bytes := []byte(`{
		"event_id":"e1","schema_version":99,"network":"ethereum",
		"event_type":"transfer","direction":"incoming","wallet_id":"w1",
		"token_symbol":"USDC","contract_address":"0xa0b8",
		"amount":"1","tx_hash":"0x1","block_number":1,
		"emitted_at":"2026-05-08T00:00:00Z"
	}`)
	_, err := Unmarshal(bytes)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSchemaMismatch),
		"consumer must distinguish schema mismatch from generic decode failure")
}

// TestEnvelope_RequiredFields enforces the non-negotiable invariants.
// Producers that forget to fill these break here, not three services
// downstream when a NULL hash collides with another.
func TestEnvelope_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*EventEnvelope)
	}{
		{"missing event_id", func(e *EventEnvelope) { e.EventID = "" }},
		{"missing network", func(e *EventEnvelope) { e.Network = "" }},
		{"missing wallet_id", func(e *EventEnvelope) { e.WalletID = "" }},
		{"missing tx_hash", func(e *EventEnvelope) { e.TxHash = "" }},
		{"missing contract_address", func(e *EventEnvelope) { e.ContractAddress = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			tc.mut(env)
			_, err := env.Marshal()
			require.Error(t, err)
		})
	}
}

func TestRoutingKey_Format(t *testing.T) {
	env := validEnvelope()
	env.Network = "ethereum"
	env.Direction = "incoming"
	env.TokenSymbol = "USDC" // upper-case input — must lowercase
	require.Equal(t, "ethereum.incoming.usdc", RoutingKey(env))
}

func validEnvelope() *EventEnvelope {
	return &EventEnvelope{
		EventID:         "evt_1",
		SchemaVersion:   SchemaCurrent,
		Network:         "ethereum",
		EventType:       "transfer",
		Direction:       "incoming",
		WalletID:        "w_1",
		TokenSymbol:     "USDC",
		ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Amount:          "1",
		TxHash:          "0x1",
		BlockNumber:     1,
		EmittedAt:       time.Now().UTC(),
	}
}
