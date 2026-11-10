package columnstore

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

var allSelect = []Column{Tenant, Warehouse, Category, Day, Units, NetCents}
var referenceFilters = []struct {
	source string
	test   func(Fact) bool
}{
	{"units >= 0 AND warehouse = 'east'", func(r Fact) bool { return r.Units != nil && *r.Units >= 0 && r.Warehouse == "east" }},
	{"category IS NULL OR net_cents > 500", func(r Fact) bool { return r.Category == nil || r.NetCents != nil && *r.NetCents > 500 }},
	{"NOT (units > 1 OR net_cents = NULL)", func(Fact) bool { return false }},
	{"category != ''", func(r Fact) bool { return r.Category != nil && *r.Category != "" }},
}

func refProjection(r Fact) Projected {
	m := map[Column]Value{Tenant: {Kind: "string", Valid: true, S: r.Tenant}, Warehouse: {Kind: "string", Valid: true, S: r.Warehouse}, Category: {Kind: "string"}, Day: {Kind: "int", Valid: true, I: r.Day}, Units: {Kind: "int"}, NetCents: {Kind: "int"}}
	if r.Category != nil {
		m[Category] = Value{Kind: "string", Valid: true, S: *r.Category}
	}
	if r.Units != nil {
		m[Units] = Value{Kind: "int", Valid: true, I: *r.Units}
	}
	if r.NetCents != nil {
		m[NetCents] = Value{Kind: "int", Valid: true, I: *r.NetCents}
	}
	return Projected{r.ID, m}
}
func refRows(rows map[uint64]Fact, tenant string, predicate func(Fact) bool) []Projected {
	var r []Projected
	for _, row := range rows {
		if row.Tenant == tenant && predicate(row) {
			r = append(r, refProjection(row))
		}
	}
	sort.Slice(r, func(i, j int) bool { return r[i].ID < r[j].ID })
	return r
}
func refCells(rows map[uint64]Fact, tenant string, grouping uint8) []Cell {
	m := map[CubeKey]Aggregate{}
	for _, r := range rows {
		if r.Tenant != tenant {
			continue
		}
		k := CubeKey{Tenant: tenant, Grouping: grouping}
		if grouping&1 == 0 {
			k.Warehouse = r.Warehouse
		}
		if grouping&2 == 0 && r.Category != nil {
			k.Category = *r.Category
			k.CategoryValid = true
		}
		if grouping&4 == 0 {
			k.Day = r.Day
		}
		a := m[k]
		a.Rows++
		if r.Units != nil {
			a.Units.Count++
			a.Units.Sum += *r.Units
		}
		if r.NetCents != nil {
			a.NetCents.Count++
			a.NetCents.Sum += *r.NetCents
		}
		m[k] = a
	}
	var r []Cell
	for k, a := range m {
		r = append(r, Cell{k, a})
	}
	sortCells(r)
	return r
}
func TestMutationsModesCubeAndReopenMatchRowOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "facts.bolt")
	d, e := Open(path, Options{})
	must(t, e)
	defer func() { d.Close() }()
	filters := make([]*Expression, len(referenceFilters))
	programs := make([]*Program, len(filters))
	for i, f := range referenceFilters {
		filters[i], e = Parse(f.source)
		must(t, e)
		programs[i], e = Compile(ctx, filters[i], t.TempDir())
		must(t, e)
		defer programs[i].Close()
	}
	oracle := map[uint64]Fact{}
	versions := map[uint64]uint64{}
	rng := rand.New(rand.NewSource(404))
	for step := 0; step < 240; step++ {
		i := rng.Intn(80)
		r := fixture(i)
		versions[r.ID]++
		r.Version = versions[r.ID]
		if rng.Intn(4) == 0 {
			must(t, d.Delete(r.Tenant, r.ID, r.Version))
			delete(oracle, r.ID)
		} else {
			r.Warehouse = []string{"east", "west", "north"}[rng.Intn(3)]
			r.Day = int64(rng.Intn(30))
			must(t, d.Put(r))
			oracle[r.ID] = r
		}
		if step%16 == 15 {
			must(t, d.Flush())
		}
		if step%50 == 49 {
			must(t, d.Compact())
		}
		if step%60 == 59 {
			must(t, d.RebuildCube())
		}
		if step%40 == 39 {
			must(t, d.Close())
			d, e = Open(path, Options{})
			must(t, e)
		}
		if step%8 != 7 {
			continue
		}
		for fi, f := range referenceFilters {
			for _, tenant := range []string{"acme", "globex"} {
				modes := []Mode{Scalar, Batch}
				if step%40 == 39 {
					modes = append(modes, Compiled)
				}
				for _, mode := range modes {
					got, e := d.Query(ctx, Query{Tenant: tenant, Filter: filters[fi], Select: allSelect, Mode: mode, Program: programs[fi]})
					must(t, e)
					if want := refRows(oracle, tenant, f.test); !reflect.DeepEqual(got.Rows, want) {
						t.Fatalf("step %d %s/%s: got %v want %v", step, tenant, mode, got.Rows, want)
					}
				}
			}
		}
		for grouping := uint8(0); grouping < 8; grouping++ {
			for _, tenant := range []string{"acme", "globex"} {
				got, e := d.Materialized(tenant, grouping)
				must(t, e)
				if want := refCells(oracle, tenant, grouping); !reflect.DeepEqual(got.Cells, want) {
					t.Fatalf("step %d cube %d differs: %v / %v", step, grouping, got.Cells, want)
				}
			}
		}
		must(t, d.Check())
	}
}
func TestLatestVersionPrecedesPredicateAndDeletesDoNotResurrect(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "facts.bolt"), Options{})
	must(t, e)
	defer d.Close()
	r := fixture(2)
	r.Tenant = "acme"
	n := int64(10)
	r.Units = &n
	must(t, d.Put(r))
	must(t, d.Flush())
	r.Version = 2
	n = -1
	must(t, d.Put(r))
	expr, e := Parse("units >= 0")
	must(t, e)
	for _, mode := range []Mode{Scalar, Batch} {
		got, e := d.Query(context.Background(), Query{Tenant: "acme", Filter: expr, Mode: mode})
		must(t, e)
		if len(got.Rows) != 0 {
			t.Fatal("old matching version resurrected")
		}
	}
	must(t, d.Flush())
	must(t, d.Delete("acme", r.ID, 3))
	must(t, d.Flush())
	must(t, d.Compact())
	if !errors.Is(d.Put(r), ErrVersion) {
		t.Fatal("stale row resurrected after compaction")
	}
	if _, e = d.Get("globex", r.ID); !errors.Is(e, kv.ErrNotFound) {
		t.Fatal("tenant leak", e)
	}
	r.Version = 4
	n = 9
	must(t, d.Put(r))
	must(t, d.Check())
}
func TestAtomicPublicationRollbackAndUnknownOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "facts.bolt")
	stage := ""
	injected := errors.New("injected")
	d, e := Open(path, Options{Failpoint: func(p string) error {
		if p == stage {
			return injected
		}
		return nil
	}})
	must(t, e)
	r := fixture(1)
	must(t, d.Put(r))
	must(t, d.Flush())
	before, e := d.Stats()
	must(t, e)
	for _, s := range []string{"ingest.ready", "flush.ready", "compact.ready", "cube.ready"} {
		stage = s
		switch s {
		case "ingest.ready":
			r.Version = 2
			e = d.Put(r)
		case "flush.ready":
			e = d.Flush()
		case "compact.ready":
			e = d.Compact()
		case "cube.ready":
			e = d.RebuildCube()
		}
		if !errors.Is(e, injected) {
			t.Fatal(s, e)
		}
		after, e := d.Stats()
		must(t, e)
		if after != before {
			t.Fatal("failed transaction changed published state", s, after, before)
		}
		must(t, d.Check())
	}
	stage = "ingest.committed"
	if e = d.Put(r); !errors.Is(e, kv.ErrRecovery) {
		t.Fatal(e)
	}
	if _, e = d.Stats(); !errors.Is(e, kv.ErrRecovery) {
		t.Fatal("uncertain handle remained usable")
	}
	must(t, d.Close())
	d, e = Open(path, Options{})
	must(t, e)
	defer d.Close()
	got, e := d.Get(r.Tenant, r.ID)
	must(t, e)
	if got.Version != 2 {
		t.Fatal("committed outcome lost")
	}
	must(t, d.Check())
}
func TestPruningProjectionNullRollupsAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "facts.bolt")
	d, e := Open(path, Options{})
	must(t, e)
	for i := 0; i < 256; i++ {
		must(t, d.Put(fixture(i)))
	}
	must(t, d.Flush())
	expr, e := Parse("warehouse = 'east' AND units IS NULL")
	must(t, e)
	got, e := d.Query(context.Background(), Query{Tenant: "acme", Filter: expr, Select: []Column{Units}, Mode: Batch})
	must(t, e)
	if got.Stats.Pruned == 0 || got.Stats.Columns >= got.Stats.Groups*uint64(len(columns)) {
		t.Fatal("pruning/projection absent", got.Stats)
	}
	for _, r := range got.Rows {
		if len(r.Values) != 1 || r.Values[Units].Valid {
			t.Fatal("null projected as zero", r)
		}
	}
	// Stored column damage must fail validation on reopen.
	must(t, d.disk.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(segmentBucket).Cursor()
		k, v := c.First()
		bad := append([]byte(nil), v...)
		bad[len(bad)-1] ^= 1
		return tx.Bucket(segmentBucket).Put(k, bad)
	}))
	must(t, d.Close())
	if _, e = Open(path, Options{}); !errors.Is(e, kv.ErrCorrupt) {
		t.Fatal("damaged column accepted", e)
	}
}
func TestConcurrentReadersAndWriter(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "facts.bolt"), Options{})
	must(t, e)
	defer d.Close()
	expr, e := Parse("true")
	must(t, e)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 16; i++ {
				r := fixture(w*16 + i)
				if e := d.Put(r); e != nil {
					t.Error(e)
					return
				}
				if _, e := d.Query(context.Background(), Query{Tenant: r.Tenant, Filter: expr, Mode: Batch}); e != nil {
					t.Error(e)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	must(t, d.Flush())
	must(t, d.Check())
}

func TestNullZeroAndAllDimensionHaveDistinctRollups(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "facts.bolt"), Options{})
	must(t, e)
	defer d.Close()
	empty, tools := "", "tools"
	zero, five := int64(0), int64(5)
	for i := 0; i < 3; i++ {
		r := Fact{ID: uint64(i + 1), Version: 1, Tenant: "acme", Warehouse: "east", Day: 0}
		if i == 1 {
			r.Category = &empty
			r.Units = &zero
		}
		if i == 2 {
			r.Category = &tools
			r.Units = &five
		}
		must(t, d.Put(r))
	}
	leaf, e := d.Materialized("acme", 0)
	must(t, e)
	if len(leaf.Cells) != 3 {
		t.Fatal("null category merged with empty or a named category", leaf)
	}
	for _, c := range leaf.Cells {
		if !c.Key.CategoryValid && c.Aggregate.Units.SQLSum().Valid {
			t.Fatal("all-null sum became zero")
		}
		if c.Key.CategoryValid && c.Key.Category == "" && (!c.Aggregate.Units.SQLSum().Valid || c.Aggregate.Units.Sum != 0) {
			t.Fatal("zero sum became null")
		}
	}
	total, e := d.Materialized("acme", 7)
	must(t, e)
	if len(total.Cells) != 1 || total.Cells[0].Key.Grouping != 7 || total.Cells[0].Aggregate.Rows != 3 {
		t.Fatal(total)
	}
	avg, valid := total.Cells[0].Aggregate.Units.Average()
	if !valid || avg != 2.5 {
		t.Fatal("null rows included in average", avg)
	}
	must(t, d.Check())
}

func TestTenantGuardProgramIdentityAndCancellation(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "facts.bolt"), Options{})
	must(t, e)
	defer d.Close()
	for i := 0; i < 8; i++ {
		must(t, d.Put(fixture(i)))
	}
	must(t, d.Flush())
	expr, e := Parse("tenant = 'globex' OR true")
	must(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p, e := Compile(ctx, expr, t.TempDir())
	must(t, e)
	defer p.Close()
	for _, mode := range []Mode{Scalar, Batch, Compiled} {
		r, e := d.Query(ctx, Query{Tenant: "acme", Filter: expr, Select: []Column{Tenant}, Mode: mode, Program: p})
		must(t, e)
		if len(r.Rows) != 4 {
			t.Fatal("tenant guard lost rows", mode, r.Rows)
		}
		for _, row := range r.Rows {
			if row.Values[Tenant].S != "acme" {
				t.Fatal("Boolean predicate escaped tenant guard")
			}
		}
	}
	other, e := Parse("true")
	must(t, e)
	if _, e = d.Query(ctx, Query{Tenant: "acme", Filter: other, Mode: Compiled, Program: p}); !errors.Is(e, ErrProgram) {
		t.Fatal("mismatched native program accepted", e)
	}
	if _, e = d.Query(ctx, Query{Tenant: "", Filter: expr, Mode: Batch}); !errors.Is(e, kv.ErrInvalid) {
		t.Fatal("missing tenant accepted", e)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, e = d.Query(canceled, Query{Tenant: "acme", Filter: expr, Mode: Compiled, Program: p}); !errors.Is(e, context.Canceled) {
		t.Fatal("canceled query ran", e)
	}
	must(t, d.Check())
}
