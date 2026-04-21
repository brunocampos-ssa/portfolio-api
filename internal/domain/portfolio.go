package domain

// Holding represents a single wallet's value in the portfolio.
type Holding struct {
	Blockchain string  `json:"blockchain"`
	Address    string  `json:"address"`
	Asset      string  `json:"asset"`
	Balance    float64 `json:"balance"`
	PriceUSD   float64 `json:"price_usd"`
	ValueUSD   float64 `json:"value_usd"`
	Error      string  `json:"error,omitempty"` // partial failure: shows why this wallet failed
}

// Portfolio represents the aggregated portfolio summary for a user.
type Portfolio struct {
	UserID   string    `json:"user_id"`
	UserName string    `json:"user_name"`
	Holdings []Holding `json:"holdings"`
	TotalUSD float64   `json:"total_usd"`
}
