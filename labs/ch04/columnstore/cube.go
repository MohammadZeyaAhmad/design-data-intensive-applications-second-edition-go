package columnstore

import (
	"encoding/json"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

// Grouping bits distinguish rolled-up dimensions from actual null/empty values.
// Tenant is never rolled up, so no cached cell aggregates different tenants.
const (
	AllWarehouses uint8 = 1
	AllCategories uint8 = 2
	AllDays       uint8 = 4
)

type CubeKey struct {
	Tenant        string `json:"tenant"`
	Warehouse     string `json:"warehouse"`
	Category      string `json:"category"`
	CategoryValid bool   `json:"category_valid"`
	Day           int64  `json:"day"`
	Grouping      uint8  `json:"grouping"`
}
type Measure struct {
	Sum   int64  `json:"sum"`
	Count uint64 `json:"nonnull_count"`
}

func (m Measure) Average() (float64, bool) {
	if m.Count == 0 {
		return 0, false
	}
	return float64(m.Sum) / float64(m.Count), true
}

func (m Measure) SQLSum() Value {
	if m.Count == 0 {
		return null("int")
	}
	return number(m.Sum)
}

type Aggregate struct {
	Rows     uint64  `json:"rows"`
	Units    Measure `json:"units"`
	NetCents Measure `json:"net_cents"`
}
type Cell struct {
	Key       CubeKey   `json:"key"`
	Aggregate Aggregate `json:"aggregate"`
}
type CubeResult struct {
	StateVersion uint64 `json:"state_version"`
	Cells        []Cell `json:"cells"`
}

func cubeKey(r Fact, grouping uint8) CubeKey {
	k := CubeKey{Tenant: r.Tenant, Grouping: grouping}
	if grouping&AllWarehouses == 0 {
		k.Warehouse = r.Warehouse
	}
	if grouping&AllCategories == 0 && r.Category != nil {
		k.Category = *r.Category
		k.CategoryValid = true
	}
	if grouping&AllDays == 0 {
		k.Day = r.Day
	}
	return k
}
func keyBytes(k CubeKey) []byte { b, _ := json.Marshal(k); return b }
func adjust(a Aggregate, r Fact, sign int64) (Aggregate, error) {
	if sign < 0 {
		if a.Rows == 0 {
			return a, kv.ErrCorrupt
		}
		a.Rows--
	} else {
		a.Rows++
	}
	change := func(m *Measure, p *int64) error {
		if p == nil {
			return nil
		}
		if sign < 0 {
			if m.Count == 0 {
				return kv.ErrCorrupt
			}
			m.Count--
		} else {
			m.Count++
		}
		m.Sum += sign * *p
		return nil
	}
	if e := change(&a.Units, r.Units); e != nil {
		return a, e
	}
	if e := change(&a.NetCents, r.NetCents); e != nil {
		return a, e
	}
	return a, nil
}
func adjustCube(tx *bolt.Tx, r Fact, sign int64) error {
	b := tx.Bucket(cubeBucket)
	for grouping := uint8(0); grouping < 8; grouping++ {
		k := keyBytes(cubeKey(r, grouping))
		var a Aggregate
		if v := b.Get(k); v != nil {
			if json.Unmarshal(v, &a) != nil {
				return kv.ErrCorrupt
			}
		}
		a, e := adjust(a, r, sign)
		if e != nil {
			return e
		}
		if a.Rows == 0 {
			if e = b.Delete(k); e != nil {
				return e
			}
		} else {
			v, _ := json.Marshal(a)
			if e = b.Put(k, v); e != nil {
				return e
			}
		}
	}
	return nil
}
func fullCube(rows []Fact) (map[string]Aggregate, error) {
	m := map[string]Aggregate{}
	for _, r := range rows {
		for grouping := uint8(0); grouping < 8; grouping++ {
			k := string(keyBytes(cubeKey(r, grouping)))
			a, e := adjust(m[k], r, 1)
			if e != nil {
				return nil, e
			}
			m[k] = a
		}
	}
	return m, nil
}
func sortCells(cells []Cell) {
	sort.Slice(cells, func(i, j int) bool { return string(keyBytes(cells[i].Key)) < string(keyBytes(cells[j].Key)) })
}

// AggregateResult groups a filtered projection. It requires all dimensions and
// measures; materialized unfiltered cells cannot answer arbitrary predicates.
func AggregateResult(r Result, grouping uint8) ([]Cell, error) {
	if grouping > 7 || !validName(r.Tenant) {
		return nil, kv.ErrInvalid
	}
	m := map[string]Cell{}
	for _, row := range r.Rows {
		for _, c := range []Column{Warehouse, Category, Day, Units, NetCents} {
			v, ok := row.Values[c]
			if !ok || v.Kind != kind(c) {
				return nil, kv.ErrInvalid
			}
		}
		get := func(c Column) Value { return row.Values[c] }
		f := Fact{ID: row.ID, Version: 1, Tenant: r.Tenant, Warehouse: get(Warehouse).S, Day: get(Day).I}
		if v := get(Category); v.Valid {
			s := v.S
			f.Category = &s
		}
		if v := get(Units); v.Valid {
			n := v.I
			f.Units = &n
		}
		if v := get(NetCents); v.Valid {
			n := v.I
			f.NetCents = &n
		}
		if !f.valid() {
			return nil, kv.ErrInvalid
		}
		key := cubeKey(f, grouping)
		k := string(keyBytes(key))
		a, e := adjust(m[k].Aggregate, f, 1)
		if e != nil {
			return nil, e
		}
		m[k] = Cell{key, a}
	}
	var cells []Cell
	for _, cell := range m {
		cells = append(cells, cell)
	}
	sortCells(cells)
	return cells, nil
}
