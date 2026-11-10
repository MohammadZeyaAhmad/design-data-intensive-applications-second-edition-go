package indexes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/storage"
)

type Measurement struct {
	Repetition     int    `json:"repetition"`
	WriteNanos     int64  `json:"write_nanos"`
	QueryNanos     int64  `json:"query_nanos"`
	Queries        int    `json:"queries"`
	Cost           Cost   `json:"algorithmic_query_cost"`
	DiskBytes      int64  `json:"disk_bytes"`
	LiveJSONBytes  int    `json:"live_row_json_bytes"`
	Rows           int    `json:"live_rows"`
	Digest         string `json:"query_results_sha256"`
	ReopenVerified bool   `json:"reopen_verified"`
}
type LayoutResult struct {
	Layout      Layout        `json:"layout"`
	Durability  string        `json:"durability"`
	Description string        `json:"description"`
	Runs        []Measurement `json:"runs"`
}
type Report struct {
	Passed      bool           `json:"passed"`
	DatasetRows int            `json:"dataset_rows"`
	Repetitions int            `json:"repetitions"`
	Environment map[string]any `json:"environment"`
	Results     []LayoutResult `json:"results"`
	Assertions  []string       `json:"assertions"`
}

func SyntheticRow(i int) Row {
	return Row{ID: uint64(i + 1), Tenant: fmt.Sprintf("tenant-%d", i%4), Warehouse: fmt.Sprintf("warehouse-%d", (i/4)%3), SKU: uint64(i + 1), OnHand: int64(i*37%100 + 10), Reserved: 3, Allocated: 2, Version: 1}
}
func reference(m map[uint64]Row, q Query) []Projection {
	var p []Projection
	for _, r := range m {
		if matches(r, q) {
			p = append(p, project(r))
		}
	}
	order(p)
	if len(p) > q.Limit {
		p = p[:q.Limit]
	}
	return p
}
func RunScenarioAt(parent string, rows, repetitions int) (Report, error) {
	var report Report
	if rows < 32 || rows > 4096 || repetitions < 1 || repetitions > 5 {
		return report, kv.ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return report, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-indexes-")
	if e != nil {
		return report, e
	}
	defer os.RemoveAll(dir)
	report = Report{DatasetRows: rows, Repetitions: repetitions, Environment: storage.Environment(dir)}
	descriptions := map[Layout]string{
		Heap:      "rows keyed by surrogate ID; decode/filter all rows, then sort and limit",
		Clustered: "full rows ordered by (tenant, warehouse, available, SKU, ID); ID-to-key locator",
		Secondary: "ordered composite secondary entries locate rows stored by ID",
		Covering:  "same secondary key plus version payload; query needs no primary-row lookup",
		Memory:    "volatile ID hash map plus sorted composite-key slice; binary-search reads and O(n) insertion shifts",
	}
	var expected string
	for _, layout := range []Layout{Heap, Clustered, Secondary, Covering, Memory} {
		result := LayoutResult{Layout: layout, Durability: "bbolt synchronous transaction per mutation", Description: descriptions[layout]}
		if layout == Memory {
			result.Durability = "volatile; all rows and index entries lost on close"
		}
		for rep := 0; rep < repetitions; rep++ {
			r, e := runLayout(filepath.Join(dir, fmt.Sprintf("%s-%d.bolt", layout, rep)), layout, rows, rep)
			if e != nil {
				return report, fmt.Errorf("%s/%d: %w", layout, rep, e)
			}
			if expected == "" {
				expected = r.Digest
			}
			if r.Digest != expected {
				return report, errors.New("index layouts disagree")
			}
			result.Runs = append(result.Runs, r)
		}
		report.Results = append(report.Results, result)
	}
	report.Assertions = []string{"all layouts return identical filtered, ordered and limited projections after rekeys and deletes", "secondary keys and covering payloads change atomically with their primary row", "natural warehouse/SKU identity conflicts roll back without removing old index entries", "tenant/warehouse leading equality fields bound the ordered range", "secondary queries perform one primary lookup per returned row; covering queries perform none", "durable layouts retain queries across close/open; memory is explicitly volatile", "algorithmic counters describe row/index work, not physical disk I/O; timings do not equate memory durability with synchronous storage"}
	report.Passed = true
	return report, nil
}
func runLayout(path string, layout Layout, n, rep int) (result Measurement, err error) {
	result.Repetition = rep
	d, e := Open(path, layout)
	if e != nil {
		return result, e
	}
	defer func() {
		if d != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	oracle := map[uint64]Row{}
	ids := rand.New(rand.NewSource(int64(4103 + rep))).Perm(n)
	for _, i := range ids {
		r := SyntheticRow(i)
		start := time.Now()
		e = d.Put(r)
		result.WriteNanos += time.Since(start).Nanoseconds()
		if e != nil {
			return result, e
		}
		oracle[r.ID] = r
	}
	for i := 0; i < n; i += 5 {
		r := SyntheticRow(i)
		r.Tenant = fmt.Sprintf("tenant-%d", (i+1)%4)
		r.OnHand += 23
		r.Version = 2
		start := time.Now()
		e = d.Put(r)
		result.WriteNanos += time.Since(start).Nanoseconds()
		if e != nil {
			return result, e
		}
		oracle[r.ID] = r
	}
	for i := 0; i < n; i += 11 {
		id := uint64(i + 1)
		start := time.Now()
		e = d.Delete(id)
		result.WriteNanos += time.Since(start).Nanoseconds()
		if e != nil {
			return result, e
		}
		delete(oracle, id)
	}
	var queries []Query
	for tenant := 0; tenant < 4; tenant++ {
		for warehouse := 0; warehouse < 3; warehouse++ {
			for _, min := range []int64{0, 25, 75} {
				queries = append(queries, Query{fmt.Sprintf("tenant-%d", tenant), fmt.Sprintf("warehouse-%d", warehouse), min, min + 50, 32})
			}
		}
	}
	queries = append(queries, Query{"absent", "warehouse-0", 0, 1000, 32}, Query{"tenant-0", "warehouse-0", 0, 1000, 32})
	h := sha256.New()
	check := func(measure bool) error {
		for _, q := range queries {
			start := time.Now()
			got, c, e := d.Query(q)
			elapsed := time.Since(start).Nanoseconds()
			if e != nil {
				return e
			}
			if want := reference(oracle, q); !reflect.DeepEqual(got, want) {
				return errors.New("query differs from row-scan reference")
			}
			if layout == Covering && (c.PrimaryLookups != 0 || c.RowsDecoded != 0) {
				return errors.New("covering query fetched a primary row")
			}
			if layout == Secondary && c.PrimaryLookups != uint64(len(got)) {
				return errors.New("secondary row fetch count differs")
			}
			if measure {
				result.QueryNanos += elapsed
				result.Queries++
				result.Cost.add(c)
				b, _ := json.Marshal(got)
				h.Write(b)
				h.Write([]byte{0})
			}
		}
		return nil
	}
	// Warm the same query set, then measure only method time, excluding oracle work.
	if e = check(false); e != nil {
		return result, e
	}
	if e = check(true); e != nil {
		return result, e
	}
	result.Digest = hex.EncodeToString(h.Sum(nil))
	result.Rows = len(oracle)
	var sortedIDs []uint64
	for id := range oracle {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
	for _, id := range sortedIDs {
		b, _ := json.Marshal(oracle[id])
		result.LiveJSONBytes += len(b)
	}
	// Exercise conflict rollback in the report itself as well as the fault tests.
	// Run after measurements so the rejected operation is outside timing/counters.
	a, b := oracle[sortedIDs[0]], oracle[sortedIDs[1]]
	conflict := b
	conflict.Tenant, conflict.Warehouse, conflict.SKU = a.Tenant, a.Warehouse, a.SKU
	if e = d.Put(conflict); !errors.Is(e, ErrConflict) {
		return result, errors.New("natural identity conflict was accepted")
	}
	got, e := d.Get(b.ID)
	if e != nil || got != b {
		return result, errors.New("rejected rekey changed its primary row")
	}
	if e = check(false); e != nil {
		return result, e
	}
	result.DiskBytes, e = d.DiskBytes()
	if e != nil {
		return result, e
	}
	if layout != Memory {
		if e = d.Close(); e != nil {
			return result, e
		}
		d = nil
		d, e = Open(path, layout)
		if e != nil {
			return result, e
		}
		if e = check(false); e != nil {
			return result, e
		}
		result.ReopenVerified = true
	}
	return result, nil
}
