package columnstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math/bits"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

type candidate struct {
	segment            segment
	group              int
	delta              []Fact
	cols               map[Column]vector
	eligible, selected uint64
}

func (c *candidate) n() int {
	if c.delta != nil {
		return len(c.delta)
	}
	return c.segment.Groups[c.group].Rows
}
func (c *candidate) load(col Column, stats *ScanStats) (vector, error) {
	if v, ok := c.cols[col]; ok {
		return v, nil
	}
	var v vector
	var e error
	if c.delta != nil {
		v.Equal = map[string]uint64{}
		for i, r := range c.delta {
			x := r.value(col)
			v.Values = append(v.Values, x)
			if x.Valid {
				v.Valid |= uint64(1) << i
				if x.Kind == "string" {
					v.Equal[x.S] |= uint64(1) << i
				}
			}
		}
	} else {
		v, e = c.segment.column(c.group, col)
		if e != nil {
			return v, e
		}
		stats.Columns++
		stats.ColumnBytes += uint64(c.segment.Groups[c.group].Columns[col].Length)
	}
	c.cols[col] = v
	return v, nil
}
func validQuery(q Query) bool {
	if !validName(q.Tenant) || q.Filter == nil || q.Filter.root == nil || q.Filter.key == "" || q.Mode != Scalar && q.Mode != Batch && q.Mode != Compiled || len(q.Select) > len(columns) {
		return false
	}
	seen := map[Column]bool{}
	for _, c := range q.Select {
		if kind(c) == "" || seen[c] {
			return false
		}
		seen[c] = true
	}
	return true
}
func (d *DB) Query(ctx context.Context, q Query) (result Result, err error) {
	if !validQuery(q) {
		return result, kv.ErrInvalid
	}
	if q.Mode == Compiled && (q.Program == nil || q.Program.key != q.Filter.key) {
		return result, ErrProgram
	}
	if len(q.Select) == 0 {
		q.Select = []Column{ID}
	}
	start := time.Now()
	cpu := processCPU()
	defer func() {
		result.Stats.WallNanos = time.Since(start).Nanoseconds()
		after := processCPU()
		if cpu != nil && after != nil {
			n := *after - *cpu
			result.Stats.ProcessCPUNanos = &n
		}
	}()
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(); e != nil {
		return result, e
	}
	result.Tenant = q.Tenant
	err = d.disk.View(func(tx *bolt.Tx) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		result.StateVersion = m.State
		heads := map[uint64]head{}
		if e = tx.Bucket(headBucket).ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				return kv.ErrCorrupt
			}
			var h head
			if json.Unmarshal(v, &h) != nil {
				return kv.ErrCorrupt
			}
			heads[binary.BigEndian.Uint64(k)] = h
			result.Stats.VisibilityEntries++
			return nil
		}); e != nil {
			return e
		}
		var candidates []*candidate
		guard := &node{Op: "=", Column: Tenant, Literal: text(q.Tenant)}
		for _, sid := range m.Segments {
			s, e := readCatalog(tx.Bucket(segmentBucket).Get(idBytes(sid)))
			if e != nil {
				return e
			}
			result.Stats.MetadataBytes += uint64(s.payload)
			for gi, g := range s.Groups {
				if e = ctx.Err(); e != nil {
					return e
				}
				result.Stats.Groups++
				if !possible(guard, g) || !possible(q.Filter.root, g) {
					result.Stats.Pruned++
					continue
				}
				result.Stats.Examined += uint64(g.Rows)
				c := &candidate{segment: s, group: gi, cols: map[Column]vector{}}
				ids, e := c.load(ID, &result.Stats)
				if e != nil {
					return e
				}
				versions, e := c.load(Version, &result.Stats)
				if e != nil {
					return e
				}
				tenants, e := c.load(Tenant, &result.Stats)
				if e != nil {
					return e
				}
				for i := 0; i < g.Rows; i++ {
					id := uint64(ids.Values[i].I)
					h, ok := heads[id]
					if ok && !h.Deleted && h.Tenant == q.Tenant && tenants.Values[i].S == q.Tenant && h.Version == uint64(versions.Values[i].I) && h.Segment == sid && h.Group == gi && h.Offset == i {
						c.eligible |= uint64(1) << i
					}
				}
				if c.eligible != 0 {
					result.Stats.Visible += uint64(bits.OnesCount64(c.eligible))
					candidates = append(candidates, c)
				}
			}
		}
		var pending []Fact
		if e = tx.Bucket(deltaBucket).ForEach(func(k, v []byte) error {
			if e := ctx.Err(); e != nil {
				return e
			}
			result.Stats.DeltaBytes += uint64(len(v))
			if len(k) != 8 {
				return kv.ErrCorrupt
			}
			r, e := readDelta(v)
			if e != nil || r.ID != binary.BigEndian.Uint64(k) {
				return kv.ErrCorrupt
			}
			if r.Deleted {
				return nil
			}
			result.Stats.Examined++
			h, ok := heads[r.ID]
			if !ok || h.Version != r.Version || h.Deleted || h.Segment != 0 {
				return kv.ErrCorrupt
			}
			if h.Tenant == q.Tenant && r.Row.Tenant == q.Tenant {
				pending = append(pending, *r.Row)
			}
			return nil
		}); e != nil {
			return e
		}
		for start := 0; start < len(pending); start += GroupRows {
			rows := pending[start:min(start+GroupRows, len(pending))]
			candidates = append(candidates, &candidate{delta: rows, cols: map[Column]vector{}, eligible: mask(len(rows))})
			result.Stats.Visible += uint64(len(rows))
		}
		refs := q.Filter.refs()
		for _, c := range candidates {
			for _, col := range refs {
				if _, e = c.load(col, &result.Stats); e != nil {
					return e
				}
			}
		}
		if q.Mode == Compiled && len(candidates) > 0 {
			var batches []wireBatch
			indices := make([][]int, len(candidates))
			for ci, c := range candidates {
				b := wireBatch{Columns: map[Column][]Value{}}
				for i := 0; i < c.n(); i++ {
					if c.eligible&(uint64(1)<<i) == 0 {
						continue
					}
					indices[ci] = append(indices[ci], i)
					for _, col := range refs {
						b.Columns[col] = append(b.Columns[col], c.cols[col].Values[i])
					}
					b.N++
				}
				batches = append(batches, b)
			}
			selected, child, e := q.Program.run(ctx, q.Filter, batches)
			result.Stats.ChildCPUNanos = child
			if e != nil {
				return e
			}
			for ci, m := range selected {
				for i, source := range indices[ci] {
					if m&(uint64(1)<<i) != 0 {
						candidates[ci].selected |= uint64(1) << source
					}
				}
			}
		} else {
			for _, c := range candidates {
				if q.Mode == Batch {
					c.selected = evalBatch(q.Filter.root, c.cols, mask(c.n())).T & c.eligible
				} else {
					for i := 0; i < c.n(); i++ {
						if c.eligible&(uint64(1)<<i) == 0 {
							continue
						}
						if eval(q.Filter.root, func(col Column) Value { return c.cols[col].Values[i] }) == trueValue {
							c.selected |= uint64(1) << i
						}
					}
				}
			}
		}
		for _, c := range candidates {
			if e = ctx.Err(); e != nil {
				return e
			}
			if c.selected == 0 {
				continue
			}
			if _, e = c.load(ID, &result.Stats); e != nil {
				return e
			}
			for _, col := range q.Select {
				if _, e = c.load(col, &result.Stats); e != nil {
					return e
				}
			}
			for i := 0; i < c.n(); i++ {
				if c.selected&(uint64(1)<<i) == 0 {
					continue
				}
				r := Projected{ID: uint64(c.cols[ID].Values[i].I), Values: map[Column]Value{}}
				for _, col := range q.Select {
					r.Values[col] = c.cols[col].Values[i]
				}
				result.Rows = append(result.Rows, r)
			}
		}
		return nil
	})
	sort.Slice(result.Rows, func(i, j int) bool { return result.Rows[i].ID < result.Rows[j].ID })
	return result, err
}
