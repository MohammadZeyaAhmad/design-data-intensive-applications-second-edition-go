package btree

import "bytes"

// Check validates occupancy, separator minima, depth, leaf links and the exact
// partition of allocated pages into reachable tree pages and free pages.
func (d *DB) Check() (CheckReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return CheckReport{}, e
	}
	return d.check(d.meta, nil, true)
}
func (d *DB) check(m metadata, overlay map[uint64][]byte, exactSize bool) (CheckReport, error) {
	var result CheckReport
	if e := validMeta(m); e != nil {
		return result, e
	}
	if m.ID != d.control.ID || m.Order != d.control.Order {
		return result, corrupt("metadata/control mismatch")
	}
	if exactSize {
		size, e := d.data.Size()
		if e != nil {
			return result, e
		}
		if size != int64(m.Next)*PageSize {
			return result, corrupt("data allocation size")
		}
	}
	read := func(id uint64) (*page, error) {
		if id == 0 || id >= m.Next {
			return nil, corrupt("page reference outside allocation")
		}
		b := overlay[id]
		if b == nil {
			var e error
			b, e = d.readImage(id)
			if e != nil {
				return nil, e
			}
		}
		n, e := decodePage(b, id, m.Order)
		if e != nil {
			return nil, e
		}
		if n.lsn > m.Generation {
			return nil, corrupt("future page generation")
		}
		return n, nil
	}
	seen := map[uint64]bool{}
	var leaves []*page
	var visit func(uint64, int) ([]byte, []byte, error)
	visit = func(id uint64, depth int) ([]byte, []byte, error) {
		if depth > m.Height || seen[id] {
			return nil, nil, corrupt("tree cycle, repeated child or height")
		}
		seen[id] = true
		n, e := read(id)
		if e != nil {
			return nil, nil, e
		}
		result.TreePages++
		if n.kind == kindLeaf {
			if depth != m.Height || id != m.Root && len(n.keys) < m.Order/2 {
				return nil, nil, corrupt("leaf occupancy or depth")
			}
			result.LeafPages++
			result.Keys += uint64(len(n.keys))
			leaves = append(leaves, n)
			if len(n.keys) == 0 {
				if id != m.Root {
					return nil, nil, corrupt("empty non-root leaf")
				}
				return nil, nil, nil
			}
			return n.keys[0], n.keys[len(n.keys)-1], nil
		}
		if n.kind != kindBranch || depth >= m.Height {
			return nil, nil, corrupt("branch type or depth")
		}
		minimum := m.Order/2 + 1
		if id == m.Root {
			minimum = 2
		}
		if len(n.children) < minimum {
			return nil, nil, corrupt("branch underflow")
		}
		result.BranchPages++
		var min, max []byte
		for i, child := range n.children {
			lo, hi, e := visit(child, depth+1)
			if e != nil {
				return nil, nil, e
			}
			if i == 0 {
				min = lo
			} else if !bytes.Equal(n.keys[i-1], lo) || bytes.Compare(max, lo) >= 0 {
				return nil, nil, corrupt("separator or subtree ordering")
			}
			max = hi
		}
		return min, max, nil
	}
	if _, _, e := visit(m.Root, 1); e != nil {
		return result, e
	}
	for i, n := range leaves {
		next := uint64(0)
		if i+1 < len(leaves) {
			next = leaves[i+1].id
		}
		if n.next != next {
			return result, corrupt("leaf chain differs from tree order")
		}
	}
	for id := m.Free; id != 0; {
		if seen[id] {
			return result, corrupt("free page cycle or live/free overlap")
		}
		seen[id] = true
		n, e := read(id)
		if e != nil {
			return result, e
		}
		if n.kind != kindFree {
			return result, corrupt("non-free page in free list")
		}
		result.FreePages++
		id = n.next
	}
	if result.Keys != m.Keys || result.FreePages != m.FreeCount || uint64(len(seen)) != m.Next-1 {
		return result, corrupt("key/free counts or leaked allocation")
	}
	result.Height = m.Height
	return result, nil
}
