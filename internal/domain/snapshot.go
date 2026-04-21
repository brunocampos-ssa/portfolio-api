package domain

import "time"

// WalletSnapshot represents a point-in-time snapshot of all tracked ETH wallets.
// It serves as the basis for a simulated tax report.
type WalletSnapshot struct {
	ID            string               `json:"id" db:"id"`
	ReferenceTime time.Time            `json:"reference_time" db:"reference_time"`
	Status        string               `json:"status" db:"status"` // "pending", "completed", "failed"
	Items         []WalletSnapshotItem `json:"items,omitempty"`
	CreatedAt     time.Time            `json:"created_at" db:"created_at"`
}

// WalletSnapshotItem represents a single asset holding within a snapshot.
// Each item captures the balance and USD value of one asset in one wallet.
type WalletSnapshotItem struct {
	ID              string    `json:"id" db:"id"`
	SnapshotID      string    `json:"snapshot_id" db:"snapshot_id"`
	WalletID        string    `json:"wallet_id" db:"wallet_id"`
	AssetSymbol     string    `json:"asset_symbol" db:"asset_symbol"`
	AssetType       string    `json:"asset_type" db:"asset_type"`             // "native" or "erc20"
	ContractAddress string    `json:"contract_address,omitempty" db:"contract_address"`
	Amount          float64   `json:"amount" db:"amount"`
	USDPrice        float64   `json:"usd_price" db:"usd_price"`
	USDValue        float64   `json:"usd_value" db:"usd_value"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
}
