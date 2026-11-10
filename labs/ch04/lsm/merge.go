package lsm

import (
	"bytes"
	"container/heap"
	"io"
	"sort"
)

type cursor struct {
	rows     []record
	index    int
	table    *table
	offset   int64
	head     record
	previous []byte
}
type mergeHeap []*cursor

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := bytes.Compare(h[i].head.key, h[j].head.key)
	if c == 0 {
		return h[i].head.seq > h[j].head.seq
	}
	return c < 0
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(v any)   { *h = append(*h, v.(*cursor)) }
func (h *mergeHeap) Pop() any     { a := *h; n := len(a); v := a[n-1]; *h = a[:n-1]; return v }
func (d *DB) advanceCursor(c *cursor) error {
	if c.table == nil {
		if c.index >= len(c.rows) {
			return io.EOF
		}
		c.head = c.rows[c.index]
		c.index++
		return nil
	}
	r, next, e := readRecord(countedReader{c.table.file, &d.count.ReadBytes}, c.offset, c.table.end)
	if e != nil {
		if e == io.EOF {
			return e
		}
		return corruption("table iteration %d", c.table.desc.ID)
	}
	if c.previous != nil && bytes.Compare(c.previous, r.key) >= 0 || r.seq < c.table.desc.MinSeq || r.seq > c.table.desc.MaxSeq {
		return corruption("table key order or sequence")
	}
	c.previous = clone(r.key)
	c.offset = next
	c.head = r
	return nil
}
func (d *DB) merge(inputs []descriptor, includeMem bool, visit func(record) bool) error {
	return d.mergeFrom(inputs, includeMem, nil, nil, visit)
}
func (d *DB) mergeFrom(inputs []descriptor, includeMem bool, start, end []byte, visit func(record) bool) error {
	h := mergeHeap{}
	add := func(c *cursor) error {
		e := d.advanceCursor(c)
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		heap.Push(&h, c)
		return nil
	}
	if includeMem {
		rows := d.sortedMem()
		first := sort.Search(len(rows), func(i int) bool { return bytes.Compare(rows[i].key, start) >= 0 })
		if e := add(&cursor{rows: rows, index: first}); e != nil {
			return e
		}
	}
	for _, desc := range inputs {
		if bytes.Compare(desc.Max, start) < 0 || end != nil && bytes.Compare(desc.Min, end) >= 0 {
			continue
		}
		t := d.tables[desc.ID]
		offset := int64(8)
		if len(start) > 0 {
			i := sort.Search(len(t.index.Blocks), func(i int) bool { return bytes.Compare(t.index.Blocks[i].First, start) > 0 }) - 1
			if i >= 0 {
				offset = t.index.Blocks[i].Offset
			}
		}
		if e := add(&cursor{table: t, offset: offset}); e != nil {
			return e
		}
	}
	for len(h) > 0 {
		c := heap.Pop(&h).(*cursor)
		best := c.head
		if e := add(c); e != nil {
			return e
		}
		for len(h) > 0 && bytes.Equal(h[0].head.key, best.key) {
			c = heap.Pop(&h).(*cursor)
			if c.head.seq > best.seq {
				best = c.head
			}
			if e := add(c); e != nil {
				return e
			}
		}
		if !visit(best) {
			return nil
		}
	}
	return nil
}

func (d *DB) writeOutputs(rows []record, level int, op string) ([]descriptor, error) {
	var result []descriptor
	for len(rows) > 0 {
		n, size := 0, 0
		for n < len(rows) && n < maxEntries {
			if n > 0 && size+rows[n].size() > d.opts.TargetTableBytes {
				break
			}
			size += rows[n].size()
			n++
		}
		t, e := d.writeTable(rows[:n], level, op)
		if e != nil {
			return nil, e
		}
		result = append(result, t)
		rows = rows[n:]
		if len(result) > maxTables {
			return nil, ErrLimit
		}
	}
	return result, nil
}
func (d *DB) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	if e := d.flushLocked(); e != nil {
		return d.poison(e)
	}
	if len(d.manifest.Tables) == 0 {
		return nil
	}
	if e := d.compact(d.manifest.Tables, maxLevel, true); e != nil {
		return d.poison(e)
	}
	return nil
}
func (d *DB) compact(inputs []descriptor, level int, full bool) error {
	// Merge one bounded output run at a time. Partial compactions KEEP deletes.
	// Only a full-database compaction can prove that no older value remains.
	var outputs []descriptor
	var rows []record
	size := 0
	var writeErr error
	flush := func() bool {
		if len(rows) == 0 {
			return true
		}
		desc, e := d.writeTable(rows, level, "compact")
		if e != nil {
			writeErr = e
			return false
		}
		outputs = append(outputs, desc)
		rows = nil
		size = 0
		if len(outputs) > maxTables {
			writeErr = ErrLimit
			return false
		}
		return true
	}
	target := d.opts.TargetTableBytes
	if d.opts.Style == Tiered && !full {
		target = maxDataBytes
	}
	e := d.merge(inputs, false, func(r record) bool {
		if full && r.deleted {
			return true
		}
		if len(rows) > 0 && (size+r.size() > target || len(rows) >= maxEntries) {
			if !flush() {
				return false
			}
		}
		rows = append(rows, r)
		size += r.size()
		return true
	})
	if e != nil {
		return e
	}
	if writeErr != nil {
		return writeErr
	}
	if !flush() {
		return writeErr
	}
	selected := map[uint64]bool{}
	for _, t := range inputs {
		selected[t.ID] = true
	}
	m := d.manifest
	m.Tables = nil
	for _, t := range d.manifest.Tables {
		if !selected[t.ID] {
			m.Tables = append(m.Tables, t)
		}
	}
	m.Tables = append(m.Tables, outputs...)
	if len(m.Tables) > maxTables {
		return ErrLimit
	}
	if e = d.publish(m, "compact"); e != nil {
		return e
	}
	d.count.Compactions++
	return nil
}
func (d *DB) compactAutomatic() error {
	for {
		inputs, level := d.pickCompaction()
		if len(inputs) == 0 {
			return nil
		}
		if e := d.compact(inputs, level, false); e != nil {
			return e
		}
	}
}
func overlaps(a, b descriptor) bool {
	return bytes.Compare(a.Min, b.Max) <= 0 && bytes.Compare(b.Min, a.Max) <= 0
}
func (d *DB) pickCompaction() ([]descriptor, int) {
	if d.opts.Style == Tiered {
		for level := 0; level < maxLevel; level++ {
			var ts []descriptor
			for _, t := range d.manifest.Tables {
				if t.Level == level {
					ts = append(ts, t)
				}
			}
			sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
			if len(ts) < d.opts.Fanout {
				continue
			}
			ts = ts[:d.opts.Fanout]
			var size int64
			count := 0
			for _, t := range ts {
				size += t.Size
				count += t.Count
			}
			if size <= maxDataBytes && count <= maxEntries {
				return ts, level + 1
			}
		}
		return nil, 0
	}
	for level := 0; level < maxLevel; level++ {
		var ts []descriptor
		var bytesAtLevel int64
		for _, t := range d.manifest.Tables {
			if t.Level == level {
				ts = append(ts, t)
				bytesAtLevel += t.Size
			}
		}
		budget := int64(d.opts.TargetTableBytes * d.opts.Fanout)
		for i := 1; i < level; i++ {
			budget *= int64(d.opts.Fanout)
		}
		if level == 0 && len(ts) < d.opts.Fanout || level > 0 && bytesAtLevel <= budget {
			continue
		}
		if level > 0 {
			sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
			ts = ts[:1]
		}
		rangeDesc := ts[0]
		for _, t := range ts[1:] {
			if bytes.Compare(t.Min, rangeDesc.Min) < 0 {
				rangeDesc.Min = t.Min
			}
			if bytes.Compare(t.Max, rangeDesc.Max) > 0 {
				rangeDesc.Max = t.Max
			}
		}
		for _, t := range d.manifest.Tables {
			if t.Level == level+1 && overlaps(t, rangeDesc) {
				ts = append(ts, t)
			}
		}
		return ts, level + 1
	}
	return nil, 0
}
