package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"
)

type Sample struct {
	Mode       string `json:"mode"`
	Query      int    `json:"query"`
	Repetition int    `json:"repetition"`
	Rows       int    `json:"rows"`
	Digest     string `json:"digest"`
	Nanos      int64  `json:"wall_nanos"`
	Work       Work   `json:"work"`
}
type Report struct {
	Passed              bool     `json:"passed"`
	Rows                int      `json:"fixture_rows"`
	Repetitions         int      `json:"repetitions"`
	Go                  string   `json:"go"`
	Samples             []Sample `json:"samples"`
	Anomaly             bool     `json:"partial_rename_detected"`
	Repair              bool     `json:"repair_verified"`
	Delete              bool     `json:"delete_verified"`
	Reopen              bool     `json:"reopen_verified"`
	NestedDocumentBytes int      `json:"nested_document_logical_bytes"`
	VariantRowBytes     int      `json:"normalized_variant_logical_bytes"`
	Schema              bool     `json:"schema_migration_verified"`
	Definitions         []string `json:"definitions"`
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func RunScenarioAt(parent string, rows, runs int) (Report, error) {
	r := Report{Rows: rows, Repetitions: runs, Go: runtime.Version()}
	if rows < 32 || rows > 4096 || runs < 1 || runs > 5 {
		return r, ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return r, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-models-")
	if e != nil {
		return r, e
	}
	defer os.RemoveAll(dir)
	f := FixtureFor(rows)
	path := filepath.Join(dir, "models.bolt")
	s, e := Open(path, &f)
	if e != nil {
		return r, e
	}
	defer func() { s.Close() }()
	queries := []Query{{"acme", "all", "", 10000, 100}, {"globex", "category-1", "", 3000, 100}, {"acme", "all", "customer-0", 10000, 100}, {"missing", "all", "", 10000, 100}}
	for qi, q := range queries {
		want, e := s.Query("normalized", q)
		if e != nil {
			return r, e
		}
		for _, mode := range []string{"normalized", "document", "denormalized", "wide-column"} {
			for rep := 0; rep < runs; rep++ {
				start := time.Now()
				got, e := s.Query(mode, q)
				nanos := time.Since(start).Nanoseconds()
				if e != nil {
					return r, e
				}
				if !reflect.DeepEqual(got.Cards, want.Cards) {
					return r, fmt.Errorf("%s differs for query %d", mode, qi)
				}
				r.Samples = append(r.Samples, Sample{mode, qi, rep, len(got.Cards), digest(got.Cards), nanos, got.Work})
			}
		}
	}
	if e = s.RenamePartially("acme", "brand-0", "Renamed maker"); e != nil {
		return r, e
	}
	q := Query{"acme", "all", "", 1e12, 100}
	a, _ := s.Query("normalized", q)
	b, _ := s.Query("document", q)
	c, _ := s.Query("denormalized", q)
	d, _ := s.Query("wide-column", q)
	r.Anomaly = !reflect.DeepEqual(a.Cards, b.Cards) && !reflect.DeepEqual(a.Cards, c.Cards) && !reflect.DeepEqual(a.Cards, d.Cards)
	if !r.Anomaly {
		return r, fmt.Errorf("rename fixture failed to expose stale copies")
	}
	if e = s.Repair(); e != nil {
		return r, e
	}
	r.Repair = true
	for _, mode := range []string{"document", "denormalized", "wide-column"} {
		b, e = s.Query(mode, q)
		if e != nil || !reflect.DeepEqual(a.Cards, b.Cards) {
			return r, fmt.Errorf("repair differs: %s", mode)
		}
	}
	large := Document{2, f.Products[0], "Renamed maker"}
	for i := 0; i < 31; i++ {
		large.Product.Variants = append(large.Product.Variants, Variant{fmt.Sprintf("extra-%d", i), "A larger embedded option"})
	}
	large.Product.Variants[0].Label = "Revised option"
	blob, _ := json.Marshal(large)
	row, _ := json.Marshal(large.Product.Variants[0])
	r.NestedDocumentBytes = len(blob)
	r.VariantRowBytes = len(row)
	legacy, old, e := DecodeWeight([]byte(`{"schema":1,"weight_grams":"120"}`))
	if e != nil {
		return r, e
	}
	current, _, e := DecodeWeight([]byte(`{"schema":2,"weight_grams":120}`))
	if e != nil {
		return r, e
	}
	_, _, bad := DecodeWeight([]byte(`{"schema":2,"weight_grams":"120"}`))
	r.Schema = old && legacy != nil && current != nil && *legacy == *current && bad != nil
	if e = s.Delete("acme", "sku-00000"); e != nil {
		return r, e
	}
	r.Delete = true
	before := map[string][]Card{}
	for _, mode := range []string{"normalized", "document", "denormalized", "wide-column"} {
		x, e := s.Query(mode, q)
		if e != nil {
			return r, e
		}
		for _, c := range x.Cards {
			if c.SKU == "sku-00000" {
				return r, fmt.Errorf("deleted product visible")
			}
		}
		before[mode] = x.Cards
		if !reflect.DeepEqual(x.Cards, before["normalized"]) {
			return r, fmt.Errorf("delete diverged")
		}
	}
	if e = s.Close(); e != nil {
		return r, e
	}
	s, e = Open(path, nil)
	if e != nil {
		return r, e
	}
	for mode, want := range before {
		x, e := s.Query(mode, q)
		if e != nil || !reflect.DeepEqual(x.Cards, want) {
			return r, fmt.Errorf("reopen diverged: %s", mode)
		}
	}
	r.Reopen = true
	r.Definitions = []string{"identical synthetic catalog/customer favorites; bounded procedural row reference, embedded documents, repeated flat rows and query-shaped column-family keys", "Go normalized/reference rows are in memory after opening; PostgreSQL companion supplies real declarative SQL and plans", "column-family partition=(tenant,category), clustering=(big-endian nonnegative price,SKU), family qualifiers identity/options; bbolt provides ordered persistence, not Cassandra replication", "favorite selection is a tenant-qualified membership filter over a category partition; a different access pattern would need its own query table", "logical nested-document and variant-row serialization bytes are not physical pages, WAL or device write amplification", "partial reference rename deliberately leaves copied names stale; repair rebuilds all copies from normalized facts", "schema-on-read migrates only explicit legacy v1 strings to v2 integers; null/absent differ from zero", "warm process/OS cache, Go wall time and logical work counters; no universal model performance ranking"}
	r.Passed = r.Anomaly && r.Repair && r.Schema && r.Reopen && r.Delete
	return r, nil
}
