package indexes

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

var primaryBucket = []byte("primary")
var indexBucket = []byte("ordered")
var locatorBucket = []byte("locator")
var identityBucket = []byte("identity")
var metaBucket = []byte("format")

type Options struct {
	// Failpoint runs at transaction.ready after row/index changes but before the
	// bbolt commit. An error rolls the whole transaction back. Do not re-enter DB.
	// The volatile layout does not support this durable-transaction failpoint.
	Failpoint func(string) error
}

type entry struct {
	key []byte
	id  uint64
}
type DB struct {
	mu         sync.RWMutex
	disk       *bolt.DB
	path       string
	layout     Layout
	closed     bool
	rows       map[uint64]Row
	identities map[string]uint64
	ordered    []entry
	options    Options
}

func Open(path string, layout Layout) (*DB, error) {
	return OpenWithOptions(path, layout, Options{})
}
func OpenWithOptions(path string, layout Layout, options Options) (*DB, error) {
	if layout != Heap && layout != Clustered && layout != Secondary && layout != Covering && layout != Memory {
		return nil, kv.ErrInvalid
	}
	d := &DB{path: path, layout: layout, options: options}
	if layout == Memory {
		if options.Failpoint != nil {
			return nil, kv.ErrInvalid
		}
		d.rows = map[uint64]Row{}
		d.identities = map[string]uint64{}
		return d, nil
	}
	if path == "" {
		return nil, kv.ErrInvalid
	}
	b, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoSync: false})
	if err != nil {
		return nil, err
	}
	d.disk = b
	err = b.Update(func(tx *bolt.Tx) error {
		fresh := tx.Bucket(metaBucket) == nil
		if fresh {
			if k, _ := tx.Cursor().First(); k != nil {
				return kv.ErrCorrupt
			}
		}
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		if !fresh && string(meta.Get([]byte("layout-v1"))) != string(layout) {
			return kv.ErrInvalid
		}
		for _, name := range [][]byte{primaryBucket, indexBucket, locatorBucket, identityBucket} {
			if !fresh && tx.Bucket(name) == nil {
				return kv.ErrCorrupt
			}
			if _, err = tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if fresh {
			if err = meta.Put([]byte("count"), idKey(0)); err != nil {
				return err
			}
		}
		if len(meta.Get([]byte("count"))) != 8 {
			return kv.ErrCorrupt
		}
		return meta.Put([]byte("layout-v1"), []byte(layout))
	})
	if err == nil {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(path))
		if err == nil {
			err = dir.Sync()
			err = errors.Join(err, dir.Close())
		}
	}
	if err != nil {
		b.Close()
		return nil, err
	}
	return d, nil
}

func decodeRow(b []byte) (Row, error) {
	var r Row
	if len(b) == 0 {
		return r, kv.ErrNotFound
	}
	if json.Unmarshal(b, &r) != nil || !r.valid() {
		return r, kv.ErrCorrupt
	}
	return r, nil
}
func (d *DB) get(tx *bolt.Tx, id uint64) (Row, error) {
	k := idKey(id)
	if d.layout == Clustered {
		k = tx.Bucket(locatorBucket).Get(k)
		if k == nil {
			return Row{}, kv.ErrNotFound
		}
	}
	r, e := decodeRow(tx.Bucket(primaryBucket).Get(k))
	if e == nil && r.ID != id {
		return Row{}, kv.ErrCorrupt
	}
	return r, e
}
func (d *DB) Get(id uint64) (Row, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return Row{}, kv.ErrClosed
	}
	if id == 0 {
		return Row{}, kv.ErrInvalid
	}
	if d.layout == Memory {
		r, ok := d.rows[id]
		if !ok {
			return Row{}, kv.ErrNotFound
		}
		return r, nil
	}
	var r Row
	e := d.disk.View(func(tx *bolt.Tx) error { var e error; r, e = d.get(tx, id); return e })
	return r, e
}
func (d *DB) remove(tx *bolt.Tx, r Row) error {
	k := idKey(r.ID)
	if d.layout == Clustered {
		k = indexKey(r)
	}
	if e := tx.Bucket(primaryBucket).Delete(k); e != nil {
		return e
	}
	if e := tx.Bucket(identityBucket).Delete(identityKey(r)); e != nil {
		return e
	}
	if e := tx.Bucket(locatorBucket).Delete(idKey(r.ID)); e != nil {
		return e
	}
	return tx.Bucket(indexBucket).Delete(indexKey(r))
}
func (d *DB) Put(r Row) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return kv.ErrClosed
	}
	if !r.valid() {
		return kv.ErrInvalid
	}
	if d.layout == Memory {
		return d.putMemory(r)
	}
	return d.disk.Update(func(tx *bolt.Tx) error {
		identity := tx.Bucket(identityBucket)
		if owner := identity.Get(identityKey(r)); owner != nil && !bytes.Equal(owner, idKey(r.ID)) {
			return ErrConflict
		}
		old, e := d.get(tx, r.ID)
		if e != nil && !errors.Is(e, kv.ErrNotFound) {
			return e
		}
		if e == nil {
			if e = d.remove(tx, old); e != nil {
				return e
			}
		} else {
			meta := tx.Bucket(metaBucket)
			n := binary.BigEndian.Uint64(meta.Get([]byte("count")))
			if n >= MaxRows {
				return kv.ErrLimit
			}
			if e = meta.Put([]byte("count"), idKey(n+1)); e != nil {
				return e
			}
		}
		b, e := json.Marshal(r)
		if e != nil {
			return e
		}
		k := idKey(r.ID)
		if d.layout == Clustered {
			k = indexKey(r)
			if e = tx.Bucket(locatorBucket).Put(idKey(r.ID), k); e != nil {
				return e
			}
		}
		if e = tx.Bucket(primaryBucket).Put(k, b); e != nil {
			return e
		}
		if e = identity.Put(identityKey(r), idKey(r.ID)); e != nil {
			return e
		}
		if d.layout == Secondary || d.layout == Covering {
			var v []byte
			if d.layout == Covering {
				v = idKey(r.Version)
			}
			if e = tx.Bucket(indexBucket).Put(indexKey(r), v); e != nil {
				return e
			}
		}
		return d.readyToCommit()
	})
}
func (d *DB) Delete(id uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return kv.ErrClosed
	}
	if id == 0 {
		return kv.ErrInvalid
	}
	if d.layout == Memory {
		if r, ok := d.rows[id]; ok {
			d.removeMemory(r)
		}
		return nil
	}
	return d.disk.Update(func(tx *bolt.Tx) error {
		r, e := d.get(tx, id)
		if errors.Is(e, kv.ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		meta := tx.Bucket(metaBucket)
		n := binary.BigEndian.Uint64(meta.Get([]byte("count")))
		if n == 0 {
			return kv.ErrCorrupt
		}
		if e = meta.Put([]byte("count"), idKey(n-1)); e != nil {
			return e
		}
		if e = d.remove(tx, r); e != nil {
			return e
		}
		return d.readyToCommit()
	})
}
func (d *DB) readyToCommit() error {
	if d.options.Failpoint != nil {
		return d.options.Failpoint("transaction.ready")
	}
	return nil
}
func (d *DB) removeMemory(r Row) {
	k := indexKey(r)
	i := sort.Search(len(d.ordered), func(i int) bool { return bytes.Compare(d.ordered[i].key, k) >= 0 })
	copy(d.ordered[i:], d.ordered[i+1:])
	last := len(d.ordered) - 1
	d.ordered[last] = entry{}
	d.ordered = d.ordered[:last]
	delete(d.rows, r.ID)
	delete(d.identities, string(identityKey(r)))
}
func (d *DB) putMemory(r Row) error {
	if owner := d.identities[string(identityKey(r))]; owner != 0 && owner != r.ID {
		return ErrConflict
	}
	if old, ok := d.rows[r.ID]; ok {
		d.removeMemory(old)
	} else if len(d.rows) >= MaxRows {
		return kv.ErrLimit
	}
	k := indexKey(r)
	i := sort.Search(len(d.ordered), func(i int) bool { return bytes.Compare(d.ordered[i].key, k) >= 0 })
	d.ordered = append(d.ordered, entry{})
	copy(d.ordered[i+1:], d.ordered[i:])
	d.ordered[i] = entry{key: k, id: r.ID}
	d.rows[r.ID] = r
	d.identities[string(identityKey(r))] = r.ID
	return nil
}

func (d *DB) Query(q Query) ([]Projection, Cost, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var rows []Projection
	var cost Cost
	if d.closed {
		return nil, cost, kv.ErrClosed
	}
	if !q.valid() {
		return nil, cost, kv.ErrInvalid
	}
	if q.MinAvailable == q.MaxAvailable {
		return nil, cost, nil
	}
	lo, hi, plen := bounds(q)
	if d.layout == Memory {
		cost.OrderedSeeks++
		i := sort.Search(len(d.ordered), func(i int) bool { return bytes.Compare(d.ordered[i].key, lo) >= 0 })
		for ; i < len(d.ordered) && bytes.Compare(d.ordered[i].key, hi) < 0 && len(rows) < q.Limit; i++ {
			cost.IndexEntries++
			cost.MemoryLookups++
			rows = append(rows, project(d.rows[d.ordered[i].id]))
		}
		cost.Returned = uint64(len(rows))
		return rows, cost, nil
	}
	e := d.disk.View(func(tx *bolt.Tx) error {
		if d.layout == Heap {
			cost.FullScans++
			e := tx.Bucket(primaryBucket).ForEach(func(_, v []byte) error {
				r, e := decodeRow(v)
				if e != nil {
					return e
				}
				cost.RowsDecoded++
				if matches(r, q) {
					rows = append(rows, project(r))
				}
				return nil
			})
			order(rows)
			if len(rows) > q.Limit {
				rows = rows[:q.Limit]
			}
			return e
		}
		name := indexBucket
		if d.layout == Clustered {
			name = primaryBucket
		}
		c := tx.Bucket(name).Cursor()
		cost.OrderedSeeks++
		for k, v := c.Seek(lo); k != nil && bytes.Compare(k, hi) < 0 && len(rows) < q.Limit; k, v = c.Next() {
			cost.IndexEntries++
			var p Projection
			if d.layout == Covering {
				var e error
				p, e = decodeProjection(k, v, plen)
				if e != nil {
					return e
				}
			} else if d.layout == Clustered {
				r, e := decodeRow(v)
				if e != nil {
					return e
				}
				cost.RowsDecoded++
				p = project(r)
				if !bytes.Equal(k, indexKey(r)) {
					return kv.ErrCorrupt
				}
			} else {
				if len(k) != plen+24 {
					return kv.ErrCorrupt
				}
				r, e := d.get(tx, binary.BigEndian.Uint64(k[plen+16:]))
				cost.PrimaryLookups++
				cost.RowsDecoded++
				if e != nil {
					return errors.Join(kv.ErrCorrupt, e)
				}
				if !bytes.Equal(k, indexKey(r)) {
					return kv.ErrCorrupt
				}
				p = project(r)
			}
			rows = append(rows, p)
		}
		return nil
	})
	cost.Returned = uint64(len(rows))
	return rows, cost, e
}
func (d *DB) DiskBytes() (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return 0, kv.ErrClosed
	}
	if d.layout == Memory {
		return 0, nil
	}
	s, e := os.Stat(d.path)
	if e != nil {
		return 0, e
	}
	return s.Size(), nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if d.disk != nil {
		return d.disk.Close()
	}
	d.rows = nil
	d.ordered = nil
	d.identities = nil
	return nil
}
