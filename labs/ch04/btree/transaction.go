package btree

import (
	"bytes"
	"io"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskio"
)

type transaction struct {
	db    *DB
	meta  metadata
	pages map[uint64]*page
	dirty map[uint64]bool
	count Counters
}

func (d *DB) begin() *transaction {
	m := d.meta
	m.Generation++
	return &transaction{db: d, meta: m, pages: map[uint64]*page{}, dirty: map[uint64]bool{}}
}
func (t *transaction) read(id uint64) (*page, error) {
	if n := t.pages[id]; n != nil {
		return n, nil
	}
	n, e := t.db.load(id, t.db.meta)
	if e != nil {
		return nil, e
	}
	t.pages[id] = n
	return n, nil
}
func (t *transaction) edit(id uint64) (*page, error) {
	n, e := t.read(id)
	if e != nil {
		return nil, e
	}
	n.lsn = t.meta.Generation
	t.dirty[id] = true
	return n, nil
}
func (t *transaction) allocate(kind byte) (*page, error) {
	var id uint64
	if t.meta.Free != 0 {
		if t.meta.FreeCount == 0 {
			return nil, corrupt("free count underflow")
		}
		old, e := t.read(t.meta.Free)
		if e != nil {
			return nil, e
		}
		if old.kind != kindFree {
			return nil, corrupt("allocating a live page")
		}
		id = old.id
		t.meta.Free = old.next
		t.meta.FreeCount--
		t.count.FreeReuses++
	} else {
		if t.meta.Next >= MaxPages {
			return nil, ErrLimit
		}
		id = t.meta.Next
		t.meta.Next++
	}
	n := &page{id: id, lsn: t.meta.Generation, kind: kind}
	t.pages[id] = n
	t.dirty[id] = true
	return n, nil
}
func (t *transaction) free(id uint64) error {
	n, e := t.read(id)
	if e != nil {
		return e
	}
	if n.kind == kindFree || id == t.meta.Root {
		return corrupt("freeing a free/root page")
	}
	t.pages[id] = &page{id: id, lsn: t.meta.Generation, kind: kindFree, next: t.meta.Free}
	t.dirty[id] = true
	t.meta.Free = id
	t.meta.FreeCount++
	return nil
}
func (t *transaction) minimum(id uint64) ([]byte, error) {
	for depth := 0; depth < 16; depth++ {
		n, e := t.read(id)
		if e != nil {
			return nil, e
		}
		if n.kind == kindLeaf {
			if len(n.keys) == 0 {
				return nil, corrupt("empty separator child")
			}
			return clone(n.keys[0]), nil
		}
		if n.kind != kindBranch || len(n.children) == 0 {
			return nil, corrupt("invalid separator child")
		}
		id = n.children[0]
	}
	return nil, corrupt("separator descent cycle")
}
func (t *transaction) separators(n *page) error {
	n.keys = nil
	for _, id := range n.children[1:] {
		k, e := t.minimum(id)
		if e != nil {
			return e
		}
		n.keys = append(n.keys, k)
	}
	return nil
}
func addBytes(a [][]byte, i int, b []byte) [][]byte {
	a = append(a, nil)
	copy(a[i+1:], a[i:])
	a[i] = clone(b)
	return a
}
func addChild(a []uint64, i int, id uint64) []uint64 {
	a = append(a, 0)
	copy(a[i+1:], a[i:])
	a[i] = id
	return a
}

// insert returns the new right sibling when this node overflows. Separators
// are always recomputed from right-child minima, including non-splitting edits.
func (t *transaction) insert(id uint64, key, value []byte) (uint64, error) {
	n, e := t.edit(id)
	if e != nil {
		return 0, e
	}
	if n.kind == kindLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			n.values[i] = clone(value)
			return 0, nil
		}
		n.keys = addBytes(n.keys, i, key)
		n.values = addBytes(n.values, i, value)
		t.meta.Keys++
		if len(n.keys) <= t.meta.Order {
			return 0, nil
		}
		right, e := t.allocate(kindLeaf)
		if e != nil {
			return 0, e
		}
		middle := len(n.keys) / 2
		right.keys = append([][]byte(nil), n.keys[middle:]...)
		right.values = append([][]byte(nil), n.values[middle:]...)
		right.next = n.next
		n.keys = n.keys[:middle]
		n.values = n.values[:middle]
		n.next = right.id
		t.count.Splits++
		return right.id, nil
	}
	if n.kind != kindBranch {
		return 0, corrupt("insertion reached non-tree page")
	}
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
	right, e := t.insert(n.children[i], key, value)
	if e != nil {
		return 0, e
	}
	if right != 0 {
		n.children = addChild(n.children, i+1, right)
	}
	if e = t.separators(n); e != nil {
		return 0, e
	}
	if len(n.children) <= t.meta.Order+1 {
		return 0, nil
	}
	sibling, e := t.allocate(kindBranch)
	if e != nil {
		return 0, e
	}
	middle := len(n.children) / 2
	sibling.children = append([]uint64(nil), n.children[middle:]...)
	n.children = n.children[:middle]
	if e = t.separators(n); e != nil {
		return 0, e
	}
	if e = t.separators(sibling); e != nil {
		return 0, e
	}
	t.count.Splits++
	return sibling.id, nil
}
func (t *transaction) underflow(n *page) bool {
	if n.kind == kindLeaf {
		return len(n.keys) < t.meta.Order/2
	}
	return len(n.children) < t.meta.Order/2+1
}
func (t *transaction) lends(n *page) bool {
	if n.kind == kindLeaf {
		return len(n.keys) > t.meta.Order/2
	}
	return len(n.children) > t.meta.Order/2+1
}
func (t *transaction) rebalance(parent *page, index int) error {
	child, e := t.edit(parent.children[index])
	if e != nil {
		return e
	}
	if !t.underflow(child) {
		return nil
	}
	if index > 0 {
		left, e := t.read(parent.children[index-1])
		if e != nil {
			return e
		}
		if left.kind != child.kind {
			return corrupt("sibling types")
		}
		if t.lends(left) {
			left, e = t.edit(left.id)
			if e != nil {
				return e
			}
			if child.kind == kindLeaf {
				last := len(left.keys) - 1
				child.keys = addBytes(child.keys, 0, left.keys[last])
				child.values = addBytes(child.values, 0, left.values[last])
				left.keys = left.keys[:last]
				left.values = left.values[:last]
			} else {
				last := len(left.children) - 1
				child.children = addChild(child.children, 0, left.children[last])
				left.children = left.children[:last]
				if e = t.separators(left); e != nil {
					return e
				}
				if e = t.separators(child); e != nil {
					return e
				}
			}
			t.count.Borrows++
			return nil
		}
	}
	if index+1 < len(parent.children) {
		right, e := t.read(parent.children[index+1])
		if e != nil {
			return e
		}
		if right.kind != child.kind {
			return corrupt("sibling types")
		}
		if t.lends(right) {
			right, e = t.edit(right.id)
			if e != nil {
				return e
			}
			if child.kind == kindLeaf {
				child.keys = append(child.keys, clone(right.keys[0]))
				child.values = append(child.values, clone(right.values[0]))
				right.keys = right.keys[1:]
				right.values = right.values[1:]
			} else {
				child.children = append(child.children, right.children[0])
				right.children = right.children[1:]
				if e = t.separators(right); e != nil {
					return e
				}
				if e = t.separators(child); e != nil {
					return e
				}
			}
			t.count.Borrows++
			return nil
		}
	}
	left, right := child, (*page)(nil)
	remove := index + 1
	if index > 0 {
		left, e = t.edit(parent.children[index-1])
		if e != nil {
			return e
		}
		right = child
		remove = index
	} else {
		right, e = t.edit(parent.children[index+1])
		if e != nil {
			return e
		}
	}
	if left.kind != right.kind {
		return corrupt("merge sibling types")
	}
	if left.kind == kindLeaf {
		left.keys = append(left.keys, right.keys...)
		left.values = append(left.values, right.values...)
		left.next = right.next
	} else {
		left.children = append(left.children, right.children...)
		if e = t.separators(left); e != nil {
			return e
		}
	}
	parent.children = append(parent.children[:remove], parent.children[remove+1:]...)
	if e = t.free(right.id); e != nil {
		return e
	}
	t.count.Merges++
	return nil
}
func (t *transaction) remove(id uint64, key []byte) (bool, error) {
	n, e := t.edit(id)
	if e != nil {
		return false, e
	}
	if n.kind == kindLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
		if i == len(n.keys) || !bytes.Equal(n.keys[i], key) {
			return false, nil
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.values = append(n.values[:i], n.values[i+1:]...)
		t.meta.Keys--
		return true, nil
	}
	if n.kind != kindBranch {
		return false, corrupt("deletion reached non-tree page")
	}
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
	found, e := t.remove(n.children[i], key)
	if e != nil || !found {
		return found, e
	}
	if e = t.rebalance(n, i); e != nil {
		return false, e
	}
	if e = t.separators(n); e != nil {
		return false, e
	}
	return true, nil
}
func (d *DB) Put(key, value []byte) error { return d.mutate(key, value, false) }
func (d *DB) Delete(key []byte) error     { return d.mutate(key, nil, true) }
func (d *DB) mutate(key, value []byte, deleted bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	if !validKey(key) || len(value) > MaxValueBytes {
		return ErrInvalid
	}
	if d.meta.Generation == ^uint64(0) {
		return ErrLimit
	}
	t := d.begin()
	if deleted {
		found, e := t.remove(t.meta.Root, key)
		if e != nil {
			return e
		}
		if !found {
			return nil
		}
		root, e := t.read(t.meta.Root)
		if e != nil {
			return e
		}
		if root.kind == kindBranch && len(root.children) == 1 {
			old := root.id
			t.meta.Root = root.children[0]
			t.meta.Height--
			if e = t.free(old); e != nil {
				return e
			}
		}
	} else {
		right, e := t.insert(t.meta.Root, key, value)
		if e != nil {
			return e
		}
		if right != 0 {
			if t.meta.Height == 16 {
				return ErrLimit
			}
			root, e := t.allocate(kindBranch)
			if e != nil {
				return e
			}
			root.children = []uint64{t.meta.Root, right}
			if e = t.separators(root); e != nil {
				return e
			}
			t.meta.Root = root.id
			t.meta.Height++
		}
	}
	if e := d.commit(t); e != nil {
		return e
	}
	d.count.LogicalBytes += uint64(len(key) + len(value))
	return nil
}
func (d *DB) commit(t *transaction) error {
	images := map[uint64][]byte{}
	b, e := encodeMeta(t.meta)
	if e != nil {
		return e
	}
	images[0] = b
	for id := range t.dirty {
		n := t.pages[id]
		b, e := encodePage(n, t.meta.Order)
		if e != nil {
			return e
		}
		images[id] = b
	}
	r := redo{id: t.meta.ID, generation: t.meta.Generation, meta: t.meta, images: images}
	frame, e := encodeRedo(r)
	if e != nil {
		return e
	}
	size, e := d.wal.Size()
	if e != nil {
		return d.poison(e)
	}
	if size != 0 {
		return d.poison(corrupt("uncleared WAL before mutation"))
	}
	if _, e = d.wal.Seek(0, io.SeekStart); e != nil {
		return d.poison(e)
	}
	n, e := diskio.WriteAll(d.wal, frame)
	d.count.WALBytes += uint64(n)
	if e != nil {
		return d.poison(e)
	}
	if e = d.hit("wal.written"); e != nil {
		return d.poison(e)
	}
	if e = d.syncFile(d.wal); e != nil {
		return d.poison(e)
	}
	if e = d.hit("wal.synced"); e != nil {
		return d.poison(e)
	}
	if e = d.checkpoint(r, "checkpoint"); e != nil {
		return d.poison(e)
	}
	d.meta = t.meta
	d.count.Splits += t.count.Splits
	d.count.Merges += t.count.Merges
	d.count.Borrows += t.count.Borrows
	d.count.FreeReuses += t.count.FreeReuses
	return nil
}
