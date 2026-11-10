package indexes

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

var layouts = []Layout{Heap, Clustered, Secondary, Covering, Memory}

func fixture(i int) Row {
	return Row{ID: uint64(i + 1), Tenant: fmt.Sprintf("tenant-%d", i%4), Warehouse: fmt.Sprintf("warehouse-%d", (i/4)%3), SKU: uint64(i + 1), OnHand: int64(i*37%100 + 10), Reserved: 3, Allocated: 2, Version: 1}
}
func oracleQuery(m map[uint64]Row, q Query) []Projection {
	var rows []Projection
	// Independent predicate and order implementation, without encoded index keys.
	for _, r := range m {
		a := r.OnHand - r.Reserved - r.Allocated
		if r.Tenant == q.Tenant && r.Warehouse == q.Warehouse && a >= q.MinAvailable && a < q.MaxAvailable {
			rows = append(rows, Projection{r.ID, r.SKU, a, r.Version})
		}
	}
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			a, b := rows[j], rows[j-1]
			less := a.Available < b.Available || a.Available == b.Available && (a.SKU < b.SKU || a.SKU == b.SKU && a.ID < b.ID)
			if !less {
				break
			}
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
	}
	return rows
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

func TestLayoutsMatchOracleDuringRekeysDeletesAndReopen(t *testing.T) {
	for _, layout := range layouts {
		t.Run(string(layout), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rows.bolt")
			d, e := Open(path, layout)
			must(t, e)
			defer func() { d.Close() }()
			m := map[uint64]Row{}
			rng := rand.New(rand.NewSource(4103))
			for step := 0; step < 360; step++ {
				i := rng.Intn(96)
				r := fixture(i)
				if rng.Intn(4) == 0 {
					must(t, d.Delete(r.ID))
					delete(m, r.ID)
				} else {
					r.Tenant = []string{"tenant-0", "tenant-1", "tenant-2", "tenant-3", "tenant-0\x00suffix"}[rng.Intn(5)]
					r.OnHand = int64(10 + rng.Intn(100))
					r.Version = uint64(step + 1)
					must(t, d.Put(r))
					m[r.ID] = r
				}
				q := Query{Tenant: []string{"tenant-0", "tenant-1", "tenant-2", "tenant-3", "tenant-0\x00suffix"}[rng.Intn(5)], Warehouse: fmt.Sprintf("warehouse-%d", rng.Intn(3)), MinAvailable: int64(rng.Intn(30)), MaxAvailable: 85, Limit: 1 + rng.Intn(20)}
				got, _, e := d.Query(q)
				must(t, e)
				if want := oracleQuery(m, q); !reflect.DeepEqual(got, want) {
					t.Fatalf("step %d: got %v want %v", step, got, want)
				}
				if step%60 == 59 && layout != Memory {
					must(t, d.Close())
					d, e = Open(path, layout)
					must(t, e)
				}
			}
			for id, want := range m {
				got, e := d.Get(id)
				must(t, e)
				if got != want {
					t.Fatal("point lookup differs", got, want)
				}
			}
		})
	}
}
func TestAtomicIdentityConflictAndCoveringMaintenance(t *testing.T) {
	for _, layout := range layouts {
		t.Run(string(layout), func(t *testing.T) {
			d, e := Open(filepath.Join(t.TempDir(), "rows.bolt"), layout)
			must(t, e)
			defer d.Close()
			a, b := fixture(0), fixture(1)
			must(t, d.Put(a))
			must(t, d.Put(b))
			conflict := b
			conflict.Tenant = a.Tenant
			conflict.Warehouse = a.Warehouse
			conflict.SKU = a.SKU
			if e = d.Put(conflict); !errors.Is(e, ErrConflict) {
				t.Fatal(e)
			}
			got, e := d.Get(b.ID)
			must(t, e)
			if got != b {
				t.Fatal("failed rekey changed primary row")
			}
			a.OnHand += 50
			a.Version = 99
			a.Tenant = "tenant-0\x00"
			a.Warehouse = "a\x00b"
			must(t, d.Put(a))
			old, _, e := d.Query(Query{Tenant: "tenant-0", Warehouse: "warehouse-0", MinAvailable: 0, MaxAvailable: 1000, Limit: 20})
			must(t, e)
			if len(old) != 0 {
				t.Fatal("stale secondary entry", old)
			}
			q := Query{Tenant: a.Tenant, Warehouse: a.Warehouse, MinAvailable: 0, MaxAvailable: 1000, Limit: 20}
			rows, c, e := d.Query(q)
			must(t, e)
			if !reflect.DeepEqual(rows, []Projection{project(a)}) {
				t.Fatal(rows)
			}
			if layout == Covering && (c.RowsDecoded != 0 || c.PrimaryLookups != 0) {
				t.Fatal("covering query touched primary rows", c)
			}
			if layout == Secondary && c.PrimaryLookups != 1 {
				t.Fatal("secondary query omitted primary lookup", c)
			}
			must(t, d.Delete(a.ID))
			must(t, d.Delete(a.ID))
			rows, _, e = d.Query(q)
			must(t, e)
			if len(rows) != 0 {
				t.Fatal("deleted covering entry survived")
			}
		})
	}
}
func TestCompositeEncodingOrderAndPrefixIsolation(t *testing.T) {
	names := []string{"a", "a\x00", "a\x00b", "aa", "b", "é"}
	for i := 1; i < len(names); i++ {
		if bytes.Compare(prefix(names[i-1], "x"), prefix(names[i], "x")) >= 0 {
			t.Fatal("string tuple order", names[i])
		}
	}
	for _, layout := range layouts {
		t.Run(string(layout), func(t *testing.T) {
			d, e := Open(filepath.Join(t.TempDir(), "rows.bolt"), layout)
			must(t, e)
			defer d.Close()
			for i, name := range names {
				r := fixture(i)
				r.Tenant = name
				r.Warehouse = "x"
				r.OnHand = 20
				must(t, d.Put(r))
			}
			for _, name := range names {
				rows, _, e := d.Query(Query{name, "x", 15, 16, 256})
				must(t, e)
				if len(rows) != 1 {
					t.Fatal("tuple boundary leak", name, rows)
				}
			}
		})
	}
}

func TestIndexTransactionRollbackAfterChangingRowAndIndexes(t *testing.T) {
	for _, layout := range []Layout{Heap, Clustered, Secondary, Covering} {
		t.Run(string(layout), func(t *testing.T) {
			abort := errors.New("injected before commit")
			fail := false
			d, e := OpenWithOptions(filepath.Join(t.TempDir(), "rows.bolt"), layout, Options{Failpoint: func(stage string) error {
				if stage != "transaction.ready" {
					t.Fatal(stage)
				}
				if fail {
					return abort
				}
				return nil
			}})
			must(t, e)
			defer d.Close()
			r := fixture(0)
			must(t, d.Put(r))
			changed := r
			changed.OnHand += 50
			changed.Version = 2
			changed.Tenant = "new-tenant"
			fail = true
			if e = d.Put(changed); !errors.Is(e, abort) {
				t.Fatal(e)
			}
			got, e := d.Get(r.ID)
			must(t, e)
			if got != r {
				t.Fatal("failed transaction changed row", got)
			}
			q := Query{r.Tenant, r.Warehouse, 0, 1000, 20}
			rows, _, e := d.Query(q)
			must(t, e)
			if !reflect.DeepEqual(rows, []Projection{project(r)}) {
				t.Fatal("rollback left stale index", rows)
			}
			rows, _, e = d.Query(Query{changed.Tenant, changed.Warehouse, 0, 1000, 20})
			must(t, e)
			if len(rows) != 0 {
				t.Fatal("failed rekey leaked new entry")
			}
			fail = false
			must(t, d.Put(changed))
			fail = true
			if e = d.Delete(changed.ID); !errors.Is(e, abort) {
				t.Fatal(e)
			}
			got, e = d.Get(changed.ID)
			must(t, e)
			if got != changed {
				t.Fatal("failed delete removed primary")
			}
			rows, _, e = d.Query(Query{changed.Tenant, changed.Warehouse, 0, 1000, 20})
			must(t, e)
			if !reflect.DeepEqual(rows, []Projection{project(changed)}) {
				t.Fatal("failed delete removed index")
			}
		})
	}
}
func TestBoundsPersistenceAndConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.bolt")
	d, e := Open(path, Covering)
	must(t, e)
	bad := fixture(0)
	bad.Reserved = bad.OnHand + 1
	if !errors.Is(d.Put(bad), kv.ErrInvalid) {
		t.Fatal("invalid stock accepted")
	}
	bad = fixture(0)
	bad.Tenant = string([]byte{255})
	if !errors.Is(d.Put(bad), kv.ErrInvalid) {
		t.Fatal("invalid UTF-8 accepted")
	}
	if _, _, e = d.Query(Query{"a", "b", 10, 9, 1}); !errors.Is(e, kv.ErrInvalid) {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				r := fixture(w*30 + i)
				if e := d.Put(r); e != nil {
					t.Error(e)
					return
				}
				if _, _, e := d.Query(Query{r.Tenant, r.Warehouse, 0, 1000, 12}); e != nil {
					t.Error(e)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	must(t, d.Close())
	if _, e = Open(path, Secondary); !errors.Is(e, kv.ErrInvalid) {
		t.Fatal("layout mismatch", e)
	}
	d, e = Open(path, Covering)
	must(t, e)
	got, e := d.Get(1)
	must(t, e)
	if got != fixture(0) {
		t.Fatal(got)
	}
	must(t, d.Close())
	if !errors.Is(d.Put(fixture(1)), kv.ErrClosed) {
		t.Fatal("closed handle accepted write")
	}
}
