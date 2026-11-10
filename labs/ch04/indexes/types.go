// Package indexes compares warehouse query layouts with atomic index maintenance.
// Durable layouts use bbolt transactions; the memory layout is explicitly volatile.
package indexes

import (
	"encoding/binary"
	"errors"
	"sort"
	"unicode/utf8"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

type Layout string

const (
	Heap      Layout = "heap"
	Clustered Layout = "clustered"
	Secondary Layout = "secondary"
	Covering  Layout = "covering"
	Memory    Layout = "memory"
	MaxRows          = 65536
)

var ErrConflict = errors.New("warehouse/SKU already has a different row ID")

type Row struct {
	ID        uint64 `json:"id"`
	Tenant    string `json:"tenant"`
	Warehouse string `json:"warehouse"`
	SKU       uint64 `json:"sku"`
	OnHand    int64  `json:"on_hand"`
	Reserved  int64  `json:"reserved"`
	Allocated int64  `json:"allocated"`
	Version   uint64 `json:"version"`
}

func (r Row) Available() int64 { return r.OnHand - r.Reserved - r.Allocated }
func (r Row) valid() bool {
	return r.ID > 0 && validName(r.Tenant) && validName(r.Warehouse) && r.SKU > 0 && r.Version > 0 && r.OnHand >= 0 && r.OnHand <= 1_000_000_000 && r.Reserved >= 0 && r.Allocated >= 0 && r.Reserved <= r.OnHand && r.Allocated <= r.OnHand-r.Reserved
}
func validName(s string) bool { return len(s) > 0 && len(s) <= 32 && utf8.ValidString(s) }

type Query struct {
	Tenant, Warehouse          string
	MinAvailable, MaxAvailable int64 // half-open interval
	Limit                      int
}

func (q Query) valid() bool {
	return validName(q.Tenant) && validName(q.Warehouse) && q.MinAvailable >= 0 && q.MaxAvailable >= q.MinAvailable && q.MaxAvailable <= 1_000_000_001 && q.Limit >= 1 && q.Limit <= 256
}

type Projection struct {
	ID        uint64 `json:"id"`
	SKU       uint64 `json:"sku"`
	Available int64  `json:"available"`
	Version   uint64 `json:"version"`
}

func project(r Row) Projection { return Projection{r.ID, r.SKU, r.Available(), r.Version} }

// Cost counts algorithmic work, not physical page reads or device I/O.
type Cost struct {
	FullScans      uint64 `json:"full_scans"`
	OrderedSeeks   uint64 `json:"ordered_seeks"`
	IndexEntries   uint64 `json:"index_entries_examined"`
	RowsDecoded    uint64 `json:"full_rows_decoded"`
	PrimaryLookups uint64 `json:"primary_row_lookups"`
	MemoryLookups  uint64 `json:"memory_row_lookups"`
	Returned       uint64 `json:"returned"`
}

func (c *Cost) add(o Cost) {
	c.FullScans += o.FullScans
	c.OrderedSeeks += o.OrderedSeeks
	c.IndexEntries += o.IndexEntries
	c.RowsDecoded += o.RowsDecoded
	c.PrimaryLookups += o.PrimaryLookups
	c.MemoryLookups += o.MemoryLookups
	c.Returned += o.Returned
}

func idKey(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }

// Zero escaping plus terminators makes arbitrary bounded byte strings distinct
// and preserves lexical order, including embedded zero and prefix-valued names.
func appendName(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			b = append(b, 0, 255)
		} else {
			b = append(b, s[i])
		}
	}
	return append(b, 0, 0)
}
func prefix(tenant, warehouse string) []byte { return appendName(appendName(nil, tenant), warehouse) }
func identityKey(r Row) []byte {
	return binary.BigEndian.AppendUint64(prefix(r.Tenant, r.Warehouse), r.SKU)
}
func indexKey(r Row) []byte {
	b := binary.BigEndian.AppendUint64(prefix(r.Tenant, r.Warehouse), uint64(r.Available()))
	b = binary.BigEndian.AppendUint64(b, r.SKU)
	return binary.BigEndian.AppendUint64(b, r.ID)
}
func bounds(q Query) ([]byte, []byte, int) {
	p := prefix(q.Tenant, q.Warehouse)
	lo := binary.BigEndian.AppendUint64(append([]byte(nil), p...), uint64(q.MinAvailable))
	hi := binary.BigEndian.AppendUint64(append([]byte(nil), p...), uint64(q.MaxAvailable))
	return lo, hi, len(p)
}
func decodeProjection(k, v []byte, prefixLen int) (Projection, error) {
	if len(k) != prefixLen+24 || len(v) != 8 {
		return Projection{}, kv.ErrCorrupt
	}
	k = k[prefixLen:]
	p := Projection{ID: binary.BigEndian.Uint64(k[16:]), SKU: binary.BigEndian.Uint64(k[8:]), Available: int64(binary.BigEndian.Uint64(k)), Version: binary.BigEndian.Uint64(v)}
	if p.ID == 0 || p.SKU == 0 || p.Version == 0 || p.Available < 0 || p.Available > 1_000_000_000 {
		return Projection{}, kv.ErrCorrupt
	}
	return p, nil
}
func matches(r Row, q Query) bool {
	return r.Tenant == q.Tenant && r.Warehouse == q.Warehouse && r.Available() >= q.MinAvailable && r.Available() < q.MaxAvailable
}
func order(rows []Projection) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Available != b.Available {
			return a.Available < b.Available
		}
		if a.SKU != b.SKU {
			return a.SKU < b.SKU
		}
		return a.ID < b.ID
	})
}
