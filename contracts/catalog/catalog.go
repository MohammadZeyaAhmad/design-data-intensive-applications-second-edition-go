package catalog

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("catalog item or quote not found")
var ErrExpired = errors.New("quote expired")
var ErrInvalid = errors.New("invalid catalog request")
var ErrConflict = errors.New("catalog version or request conflict")

type Line struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}
type PriceLine struct {
	SKU            string `json:"sku"`
	Quantity       int64  `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	PriceVersion   int64  `json:"price_version"`
}
type Quote struct {
	ID         string      `json:"id"`
	TenantID   string      `json:"tenant_id"`
	Currency   string      `json:"currency"`
	TotalMinor int64       `json:"total_minor"`
	Lines      []PriceLine `json:"lines"`
	ExpiresAt  time.Time   `json:"expires_at"`
}
type Reader interface {
	Quote(context.Context, string, string) (Quote, error)
}
