package ports

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
)

// CachedProducts is an explicitly bounded-stale display read. Authoritative
// pricing, quotes and ordinary Product reads keep the Store contract.
type CachedProducts interface {
	CachedProduct(context.Context, string, string, int64) (domain.Product, string, error)
}
