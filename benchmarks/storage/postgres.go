// Package storagebench runs isolated PostgreSQL index-plan experiments.
// Every object is session-local in pg_temp; application tables are untouched.
package storagebench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/jackc/pgx/v5"
)

const Query = `SELECT id, sku, available, version
FROM pg_temp.atlas_index_layout
WHERE tenant = 'tenant-0' AND warehouse = 'warehouse-0'
  AND available >= 100 AND available < 500
ORDER BY available, sku, id LIMIT 40`

type Projection struct {
	ID        int64 `json:"id"`
	SKU       int64 `json:"sku"`
	Available int64 `json:"available"`
	Version   int64 `json:"version"`
}
type PlanNode struct {
	Type        string     `json:"Node Type"`
	Index       string     `json:"Index Name"`
	HeapFetches int64      `json:"Heap Fetches"`
	Plans       []PlanNode `json:"Plans"`
}
type Explain struct {
	Plan        PlanNode `json:"Plan"`
	PlanningMS  float64  `json:"Planning Time"`
	ExecutionMS float64  `json:"Execution Time"`
}
type Timing struct {
	MinMS    float64 `json:"min_ms"`
	MaxMS    float64 `json:"max_ms"`
	MeanMS   float64 `json:"mean_ms"`
	StdDevMS float64 `json:"population_stddev_ms"`
}
type PlanCase struct {
	Name      string            `json:"name"`
	DDL       string            `json:"index_ddl"`
	Plans     []json.RawMessage `json:"explain_analyze_buffers"`
	Execution Timing            `json:"execution"`
	Rows      int               `json:"rows"`
	Digest    string            `json:"results_sha256"`
}
type PlanReport struct {
	Passed           bool              `json:"passed"`
	DatasetRows      int               `json:"dataset_rows"`
	Repetitions      int               `json:"repetitions"`
	Settings         map[string]string `json:"settings"`
	Query            string            `json:"query"`
	Cases            []PlanCase        `json:"cases"`
	MutationVerified bool              `json:"covering_update_delete_verified"`
	Assertions       []string          `json:"assertions"`
}

func reference(n int, updateID, deleteID int64) []Projection {
	var rows []Projection
	for i := int64(1); i <= int64(n); i++ {
		if i%8 != 0 || (i/8)%4 != 0 || i == deleteID {
			continue
		}
		p := Projection{i, i, i * 37 % 1000, 1}
		if i == updateID {
			p.Available = 101
			p.Version = 2
		}
		if p.Available >= 100 && p.Available < 500 {
			rows = append(rows, p)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return a.Available < b.Available || a.Available == b.Available && (a.SKU < b.SKU || a.SKU == b.SKU && a.ID < b.ID)
	})
	if len(rows) > 40 {
		rows = rows[:40]
	}
	return rows
}
func query(ctx context.Context, c *pgx.Conn, want []Projection) ([]Projection, error) {
	r, e := c.Query(ctx, Query)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	var rows []Projection
	for r.Next() {
		var p Projection
		if e = r.Scan(&p.ID, &p.SKU, &p.Available, &p.Version); e != nil {
			return nil, e
		}
		rows = append(rows, p)
	}
	if e = r.Err(); e != nil {
		return nil, e
	}
	if !reflect.DeepEqual(rows, want) {
		return nil, errors.New("query plan changed the ordered result")
	}
	return rows, nil
}
func hasIndex(n PlanNode, name, kind string) bool {
	if n.Index == name && (kind == "" || n.Type == kind) {
		return true
	}
	for _, child := range n.Plans {
		if hasIndex(child, name, kind) {
			return true
		}
	}
	return false
}
func heapFetches(n PlanNode) int64 {
	v := n.HeapFetches
	for _, c := range n.Plans {
		v += heapFetches(c)
	}
	return v
}
func timing(values []float64) Timing {
	t := Timing{MinMS: values[0], MaxMS: values[0]}
	for _, v := range values {
		t.MinMS = math.Min(t.MinMS, v)
		t.MaxMS = math.Max(t.MaxMS, v)
		t.MeanMS += v / float64(len(values))
	}
	for _, v := range values {
		d := v - t.MeanMS
		t.StdDevMS += d * d / float64(len(values))
	}
	t.StdDevMS = math.Sqrt(t.StdDevMS)
	return t
}

// Run requires a dedicated connection, which the caller closes to remove the
// temporary fixture. It never changes planner enable_* settings or global caches.
func Run(ctx context.Context, c *pgx.Conn, rows, repetitions int) (PlanReport, error) {
	var report PlanReport
	if rows < 4096 || rows > 131072 || repetitions < 1 || repetitions > 5 {
		return report, errors.New("plan fixture requires 4096..131072 rows and 1..5 repetitions")
	}
	report = PlanReport{DatasetRows: rows, Repetitions: repetitions, Query: Query, Settings: map[string]string{}}
	for _, name := range []string{"server_version", "block_size", "shared_buffers", "temp_buffers", "random_page_cost", "seq_page_cost", "effective_cache_size", "enable_seqscan", "enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan", "jit", "temp_tablespaces", "fsync", "synchronous_commit"} {
		var value string
		if e := c.QueryRow(ctx, "SHOW "+name).Scan(&value); e != nil {
			return report, e
		}
		report.Settings[name] = value
	}
	report.Settings["cache"] = "warm query per index layout before repeated EXPLAIN ANALYZE; OS cache retained; temp relation uses local buffers"
	report.Settings["physical_device"] = "not identified; buffers and file sizes do not identify device I/O"
	if _, e := c.Exec(ctx, `CREATE TEMP TABLE atlas_index_layout (
id bigint PRIMARY KEY, tenant text NOT NULL, warehouse text NOT NULL, sku bigint NOT NULL,
available integer NOT NULL, version bigint NOT NULL, payload text NOT NULL
)`); e != nil {
		return report, e
	}
	if _, e := c.Exec(ctx, `INSERT INTO pg_temp.atlas_index_layout
SELECT i, 'tenant-' || (i % 8), 'warehouse-' || ((i / 8) % 4), i,
       (i * 37 % 1000)::integer, 1, repeat('warehouse-snapshot-', 32)
FROM generate_series(1, $1::bigint) i`, rows); e != nil {
		return report, e
	}
	want := reference(rows, 0, 0)
	cases := []struct{ name, ddl, drop, index, kind string }{
		{"heap", "", "", "", ""},
		{"tenant-secondary", "CREATE INDEX atlas_by_tenant ON pg_temp.atlas_index_layout (tenant)", "", "", ""},
		{"composite", "CREATE INDEX atlas_composite ON pg_temp.atlas_index_layout (tenant, warehouse, available, sku, id)", "DROP INDEX pg_temp.atlas_by_tenant", "atlas_composite", "Index Scan"},
		{"covering", "CREATE INDEX atlas_covering ON pg_temp.atlas_index_layout (tenant, warehouse, available, sku, id) INCLUDE (version)", "DROP INDEX pg_temp.atlas_composite", "atlas_covering", "Index Only Scan"},
	}
	for _, scenario := range cases {
		if scenario.drop != "" {
			if _, e := c.Exec(ctx, scenario.drop); e != nil {
				return report, e
			}
		}
		if scenario.ddl != "" {
			if _, e := c.Exec(ctx, scenario.ddl); e != nil {
				return report, e
			}
		}
		// VACUUM updates visibility bits so a covering index can avoid heap fetches.
		if _, e := c.Exec(ctx, "VACUUM (ANALYZE) pg_temp.atlas_index_layout"); e != nil {
			return report, e
		}
		got, e := query(ctx, c, want)
		if e != nil {
			return report, e
		}
		b, _ := json.Marshal(got)
		hash := sha256.Sum256(b)
		result := PlanCase{Name: scenario.name, DDL: scenario.ddl, Rows: len(got), Digest: hex.EncodeToString(hash[:])}
		var times []float64
		for run := 0; run < repetitions; run++ {
			var raw []byte
			if e = c.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+Query).Scan(&raw); e != nil {
				return report, e
			}
			var plans []Explain
			if e = json.Unmarshal(raw, &plans); e != nil || len(plans) != 1 {
				return report, errors.New("unexpected EXPLAIN format")
			}
			p := plans[0]
			if scenario.index != "" && !hasIndex(p.Plan, scenario.index, scenario.kind) {
				return report, fmt.Errorf("planner did not select %s for %s; inspect recorded settings and dataset", scenario.kind, scenario.name)
			}
			if scenario.name == "covering" && heapFetches(p.Plan) != 0 {
				return report, errors.New("vacuumed covering scan still fetches heap tuples")
			}
			result.Plans = append(result.Plans, json.RawMessage(raw))
			times = append(times, p.ExecutionMS)
		}
		result.Execution = timing(times)
		report.Cases = append(report.Cases, result)
	}
	// Real index maintenance: change both the ordering field and included payload,
	// then delete another selected row. Compare with an independent fixture model.
	updateID, deleteID := want[0].ID, want[1].ID
	if _, e := c.Exec(ctx, "UPDATE pg_temp.atlas_index_layout SET available=101,version=2 WHERE id=$1", updateID); e != nil {
		return report, e
	}
	if _, e := c.Exec(ctx, "DELETE FROM pg_temp.atlas_index_layout WHERE id=$1", deleteID); e != nil {
		return report, e
	}
	if _, e := query(ctx, c, reference(rows, updateID, deleteID)); e != nil {
		return report, e
	}
	report.MutationVerified = true
	report.Assertions = []string{"all index layouts preserve the same ordered query result", "default planner chooses a multicolumn Index Scan and a covering Index Only Scan", "vacuumed covering query reports zero heap fetches", "an updated ordering key and included version plus a delete remain visible correctly", "no enable_* planner switches are changed and no application table is modified", "raw JSON plans include buffer counts and every timing sample; warm-cache population variance is reported"}
	report.Passed = true
	return report, nil
}
