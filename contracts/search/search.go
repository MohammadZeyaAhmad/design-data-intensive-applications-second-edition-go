// Package search publishes non-authoritative discovery references. A match is
// not a price quote, inventory promise or shipment allocation.
package search

import "context"

type Hit struct {
	ID               string  `json:"id"`
	SKU              string  `json:"sku,omitempty"`
	Warehouse        string  `json:"warehouse,omitempty"`
	Title            string  `json:"title"`
	Version          int64   `json:"document_version"`
	Rank             float64 `json:"rank,omitempty"`
	Distance         float64 `json:"distance,omitempty"`
	EmbeddingVersion int64   `json:"embedding_version,omitempty"`
}
type Reader interface {
	Text(context.Context, string, string, int) ([]Hit, error)
}
