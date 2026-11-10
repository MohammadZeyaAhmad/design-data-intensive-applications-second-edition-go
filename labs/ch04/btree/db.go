package btree

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskio"
)

type control struct {
	Version int      `json:"version"`
	ID      [16]byte `json:"id"`
	Order   int      `json:"max_keys"`
	Ready   bool     `json:"ready"`
}
type controlEnvelope struct {
	Data   json.RawMessage `json:"data"`
	SHA256 string          `json:"sha256"`
}
type DB struct {
	mu        sync.Mutex
	fs        diskio.FS
	dir       string
	opts      Options
	control   control
	meta      metadata
	lock      io.Closer
	data, wal diskio.File
	count     Counters
	closed    bool
	bad       error
}

func Open(dir string, o Options) (*DB, error) {
	if dir == "" {
		return nil, ErrInvalid
	}
	path, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	return openFS(diskio.OS{}, path, o)
}
func openFS(fs diskio.FS, dir string, o Options) (db *DB, err error) {
	if o.MaxKeys != 0 && !validOrder(o.MaxKeys) {
		return nil, ErrInvalid
	}
	d := &DB{fs: fs, dir: dir, opts: o}
	if err = fs.MkdirAll(dir); err != nil {
		return nil, err
	}
	if d.lock, err = fs.Lock(d.path("LOCK")); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			d.closeLocked()
		}
	}()
	names, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		if n != "LOCK" && n != "CONTROL" && n != "CONTROL.tmp" && n != "DATA" && n != "WAL" {
			return nil, corrupt("unrecognized database file")
		}
	}
	c, err := d.readControl()
	if os.IsNotExist(err) {
		for _, n := range names {
			if n != "LOCK" && n != "CONTROL.tmp" {
				return nil, corrupt("control absent in nonempty directory")
			}
		}
		order := o.MaxKeys
		if order == 0 {
			order = 8
		}
		c = control{Version: 1, Order: order}
		if _, err = rand.Read(c.ID[:]); err != nil {
			return nil, err
		}
		if err = d.writeControl(c); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if o.MaxKeys != 0 && o.MaxKeys != c.Order {
		return nil, ErrInvalid
	}
	d.control = c
	d.opts.MaxKeys = c.Order
	flags := os.O_RDWR
	if !c.Ready {
		flags |= os.O_CREATE
	}
	d.data, err = fs.Open(d.path("DATA"), flags)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, corrupt("missing data file")
		}
		return nil, err
	}
	d.wal, err = fs.Open(d.path("WAL"), flags)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, corrupt("missing registered WAL")
		}
		return nil, err
	}
	if !c.Ready {
		m := metadata{ID: c.ID, Root: 1, Next: 2, Order: c.Order, Height: 1}
		leaf, e := encodePage(&page{id: 1, kind: kindLeaf}, m.Order)
		if e != nil {
			return nil, e
		}
		meta, e := encodeMeta(m)
		if e != nil {
			return nil, e
		}
		if err = d.data.Truncate(2 * PageSize); err != nil {
			return nil, err
		}
		if err = d.writeImage(1, leaf); err != nil {
			return nil, err
		}
		if err = d.writeImage(0, meta); err != nil {
			return nil, err
		}
		if err = d.syncFile(d.data); err != nil {
			return nil, err
		}
		if err = d.wal.Truncate(0); err != nil {
			return nil, err
		}
		if err = d.syncFile(d.wal); err != nil {
			return nil, err
		}
		if err = d.syncDir(); err != nil {
			return nil, err
		}
		c.Ready = true
		if err = d.writeControl(c); err != nil {
			return nil, err
		}
		d.control = c
	}
	if err = d.recover(); err != nil {
		return nil, err
	}
	if err = fs.Remove(d.path("CONTROL.tmp")); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err = d.syncDir(); err != nil {
		return nil, err
	}
	return d, nil
}
func (d *DB) path(n string) string { return filepath.Join(d.dir, n) }
func (d *DB) hit(p string) error {
	if d.opts.Failpoint != nil {
		return d.opts.Failpoint(p)
	}
	return nil
}
func (d *DB) ready() error {
	if d.closed {
		return ErrClosed
	}
	return d.bad
}
func (d *DB) poison(e error) error { d.bad = errors.Join(ErrRecovery, e); return d.bad }
func (d *DB) syncFile(f diskio.File) error {
	if e := f.Sync(); e != nil {
		return e
	}
	d.count.Syncs++
	return nil
}
func (d *DB) syncDir() error {
	if e := d.fs.SyncDir(d.dir); e != nil {
		return e
	}
	d.count.Syncs++
	return nil
}
func strictJSON(b []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if e := dec.Decode(target); e != nil {
		return e
	}
	var extra any
	if e := dec.Decode(&extra); e != io.EOF {
		return ErrCorrupt
	}
	return nil
}
func (d *DB) readControl() (control, error) {
	var c control
	f, e := d.fs.Open(d.path("CONTROL"), os.O_RDONLY)
	if e != nil {
		return c, e
	}
	defer f.Close()
	s, e := f.Size()
	if e != nil {
		return c, e
	}
	if s < 1 || s > 4096 {
		return c, corrupt("control size")
	}
	b := make([]byte, s)
	if _, e = f.ReadAt(b, 0); e != nil {
		return c, e
	}
	var env controlEnvelope
	if e = strictJSON(b, &env); e != nil {
		return c, corrupt("control envelope")
	}
	h := sha256.Sum256(env.Data)
	if env.SHA256 != hex.EncodeToString(h[:]) {
		return c, corrupt("control checksum")
	}
	if e = strictJSON(env.Data, &c); e != nil {
		return c, corrupt("control payload")
	}
	if c.Version != 1 || c.ID == [16]byte{} || !validOrder(c.Order) {
		return c, corrupt("control version or order")
	}
	return c, nil
}
func (d *DB) writeControl(c control) error {
	b, e := json.Marshal(c)
	if e != nil {
		return e
	}
	h := sha256.Sum256(b)
	b, e = json.Marshal(controlEnvelope{b, hex.EncodeToString(h[:])})
	if e != nil {
		return e
	}
	f, e := d.fs.Open(d.path("CONTROL.tmp"), os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = diskio.WriteAll(f, b); e != nil {
		return e
	}
	if e = d.hit("initialize.control.written"); e != nil {
		return e
	}
	if e = d.syncFile(f); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = d.fs.Rename(d.path("CONTROL.tmp"), d.path("CONTROL")); e != nil {
		return e
	}
	if e = d.hit("initialize.control.renamed"); e != nil {
		return e
	}
	return d.syncDir()
}
func (d *DB) readImage(id uint64) ([]byte, error) {
	if id >= MaxPages {
		return nil, corrupt("page address")
	}
	b := make([]byte, PageSize)
	n, e := d.data.ReadAt(b, int64(id)*PageSize)
	d.count.ReadBytes += uint64(n)
	if e != nil {
		return nil, corrupt("short page %d", id)
	}
	return b, nil
}
func (d *DB) writeImage(id uint64, b []byte) error {
	n, e := diskio.WriteAllAt(d.data, b, int64(id)*PageSize)
	d.count.DataBytes += uint64(n)
	return e
}
func (d *DB) readMeta() (metadata, error) {
	b, e := d.readImage(0)
	if e != nil {
		return metadata{}, e
	}
	m, e := decodeMeta(b)
	if e == nil && (m.ID != d.control.ID || m.Order != d.control.Order) {
		e = corrupt("data/control identity")
	}
	return m, e
}
func (d *DB) load(id uint64, m metadata) (*page, error) {
	if id == 0 || id >= m.Next {
		return nil, corrupt("page outside allocation")
	}
	b, e := d.readImage(id)
	if e != nil {
		return nil, e
	}
	n, e := decodePage(b, id, m.Order)
	if e == nil && n.lsn > m.Generation {
		e = corrupt("page from future generation")
	}
	return n, e
}
func (d *DB) recover() error {
	size, e := d.wal.Size()
	if e != nil {
		return e
	}
	if size < 0 || size > maxLogBytes {
		return corrupt("WAL size")
	}
	b := make([]byte, size)
	if size > 0 {
		if _, e = d.wal.ReadAt(b, 0); e != nil {
			return e
		}
	}
	r, e := decodeRedo(b)
	if errors.Is(e, io.EOF) || errors.Is(e, errTail) {
		m, metaErr := d.readMeta()
		if metaErr != nil {
			return metaErr
		}
		if _, metaErr = d.check(m, nil, true); metaErr != nil {
			return metaErr
		}
		d.meta = m
		if size > 0 {
			if metaErr = d.resetLog("recovery"); metaErr != nil {
				return metaErr
			}
		}
		_, metaErr = d.wal.Seek(0, io.SeekStart)
		return metaErr
	}
	if e != nil {
		return e
	}
	if r.id != d.control.ID || r.meta.Order != d.control.Order {
		return corrupt("foreign redo transaction")
	}
	old, oldErr := d.readMeta()
	if old.ID != [16]byte{} && (old.ID != d.control.ID || old.Order != d.control.Order) {
		return corrupt("foreign data metadata")
	}
	if oldErr == nil && old.Next > r.meta.Next {
		return corrupt("redo allocation regressed")
	}
	if oldErr == nil && (old.Generation != r.generation && (old.Generation == ^uint64(0) || old.Generation+1 != r.generation)) {
		return corrupt("stale or skipped redo generation")
	}
	// Validate the complete intended tree through the after-image overlay before
	// touching DATA. Torn pages and metadata covered by the WAL are repairable.
	if _, e = d.check(r.meta, r.images, false); e != nil {
		return e
	}
	if e = d.checkpoint(r, "recovery"); e != nil {
		return e
	}
	d.meta = r.meta
	d.count.Recovered++
	_, e = d.check(d.meta, nil, true)
	return e
}
func (d *DB) resetLog(op string) error {
	if e := d.wal.Truncate(0); e != nil {
		return e
	}
	if _, e := d.wal.Seek(0, io.SeekStart); e != nil {
		return e
	}
	if e := d.hit(op + ".wal.truncated"); e != nil {
		return e
	}
	if e := d.syncFile(d.wal); e != nil {
		return e
	}
	return d.hit(op + ".wal.synced")
}
func (d *DB) checkpoint(r redo, op string) error {
	ids := make([]uint64, 0, len(r.images))
	for id := range r.images {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if e := d.writeImage(id, r.images[id]); e != nil {
			return e
		}
		if e := d.hit(op + ".page.written"); e != nil {
			return e
		}
	}
	if e := d.data.Truncate(int64(r.meta.Next) * PageSize); e != nil {
		return e
	}
	if e := d.hit(op + ".data.resized"); e != nil {
		return e
	}
	if e := d.writeImage(0, r.images[0]); e != nil {
		return e
	}
	if e := d.hit(op + ".meta.written"); e != nil {
		return e
	}
	if e := d.syncFile(d.data); e != nil {
		return e
	}
	if e := d.hit(op + ".data.synced"); e != nil {
		return e
	}
	return d.resetLog(op)
}
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return nil, e
	}
	if !validKey(key) {
		return nil, ErrInvalid
	}
	id := d.meta.Root
	for depth := 1; depth <= d.meta.Height; depth++ {
		n, e := d.load(id, d.meta)
		if e != nil {
			return nil, e
		}
		switch n.kind {
		case kindLeaf:
			i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
			if i == len(n.keys) || !bytes.Equal(n.keys[i], key) {
				return nil, ErrNotFound
			}
			return clone(n.values[i]), nil
		case kindBranch:
			i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
			id = n.children[i]
		default:
			return nil, corrupt("non-tree page in search")
		}
	}
	return nil, corrupt("tree height in search")
}
func (d *DB) Scan(start, end []byte, limit int) ([]Pair, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return nil, e
	}
	if len(start) > MaxKeyBytes || len(end) > MaxKeyBytes+1 || limit < 1 || limit > kv.MaxScan || end != nil && bytes.Compare(start, end) > 0 {
		return nil, ErrInvalid
	}
	id := d.meta.Root
	var leaf *page
	for depth := 1; depth <= d.meta.Height; depth++ {
		n, e := d.load(id, d.meta)
		if e != nil {
			return nil, e
		}
		if n.kind == kindLeaf {
			leaf = n
			break
		}
		if n.kind != kindBranch {
			return nil, corrupt("range branch")
		}
		i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], start) > 0 })
		id = n.children[i]
	}
	if leaf == nil {
		return nil, corrupt("range height")
	}
	var rows []Pair
	seen := map[uint64]bool{}
	for leaf != nil {
		if seen[leaf.id] {
			return nil, corrupt("leaf chain cycle")
		}
		seen[leaf.id] = true
		for i, k := range leaf.keys {
			if bytes.Compare(k, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(k, end) >= 0 {
				return rows, nil
			}
			rows = append(rows, Pair{Key: clone(k), Value: clone(leaf.values[i])})
			if len(rows) == limit {
				return rows, nil
			}
		}
		if leaf.next == 0 {
			break
		}
		n, e := d.load(leaf.next, d.meta)
		if e != nil {
			return nil, e
		}
		if n.kind != kindLeaf {
			return nil, corrupt("non-leaf sibling")
		}
		leaf = n
	}
	return rows, nil
}
func (d *DB) Stats() (Stats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return Stats{}, e
	}
	s := Stats{Generation: d.meta.Generation, Keys: d.meta.Keys, Height: d.meta.Height, MaxKeys: d.meta.Order, AllocatedPages: d.meta.Next - 1, FreePages: d.meta.FreeCount, Counters: d.count}
	for _, n := range []string{"CONTROL", "DATA", "WAL"} {
		f, e := d.fs.Open(d.path(n), os.O_RDONLY)
		if e != nil {
			return s, e
		}
		size, e := f.Size()
		f.Close()
		if e != nil {
			return s, e
		}
		s.DiskBytes += size
	}
	return s, nil
}
func (d *DB) Close() error { d.mu.Lock(); defer d.mu.Unlock(); return d.closeLocked() }
func (d *DB) closeLocked() error {
	if d.closed {
		return nil
	}
	d.closed = true
	var es []error
	if d.wal != nil {
		es = append(es, d.wal.Close())
	}
	if d.data != nil {
		es = append(es, d.data.Close())
	}
	if d.lock != nil {
		es = append(es, d.lock.Close())
	}
	return errors.Join(es...)
}
