package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
)

const MaxValueBytes = 2048

var ErrCache = errors.New("catalog cache unavailable or corrupt")
var tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var skuPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type Snapshot struct {
	Floor     int64
	Version   int64
	Payload   []byte
	Remaining time.Duration
}
type Backend interface {
	Get(context.Context, string) (Snapshot, error)
	Put(context.Context, string, int64, []byte, time.Duration, time.Duration) (bool, error)
	Invalidate(context.Context, string, int64, time.Duration) error
	Discard(context.Context, string) error
}
type record struct {
	Format  int            `json:"format"`
	Tenant  string         `json:"tenant"`
	SKU     string         `json:"sku"`
	Missing bool           `json:"missing,omitempty"`
	Product domain.Product `json:"product"`
}

func key(tenant, sku string) string {
	hash := sha256.Sum256([]byte(tenant + "\x00" + sku))
	return hex.EncodeToString(hash[:])
}
func decode(tenant, sku string, s Snapshot) (record, error) {
	var value record
	if len(s.Payload) == 0 || len(s.Payload) > MaxValueBytes {
		return value, ErrCache
	}
	d := json.NewDecoder(bytes.NewReader(s.Payload))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || d.Decode(new(any)) != io.EOF {
		return value, ErrCache
	}
	if value.Format != 1 || value.Tenant != tenant || value.SKU != sku {
		return value, ErrCache
	}
	if value.Missing {
		if value.Product != (domain.Product{}) || s.Version != 0 {
			return value, ErrCache
		}
	} else if !domain.ValidProduct(value.Product) || value.Product.Version < 1 || value.Product.SKU != sku || value.Product.Version != s.Version {
		return value, ErrCache
	}
	if s.Version < s.Floor {
		return value, ErrCache
	}
	return value, nil
}
func encode(tenant, sku string, p domain.Product, missing bool) ([]byte, error) {
	return json.Marshal(record{Format: 1, Tenant: tenant, SKU: sku, Product: p, Missing: missing})
}
