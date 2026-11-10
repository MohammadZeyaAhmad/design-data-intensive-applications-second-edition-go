package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	mu          sync.Mutex
	fs          filesystem
	dir         string
	opts        Options
	lock        io.Closer
	wal         dataFile
	manifest    manifest
	tables      map[uint64]*table
	mem         map[string]record
	memBytes    int
	seq, nextID uint64
	count       Counters
	closed      bool
	bad         error
}

func Open(dir string, opts Options) (*DB, error) {
	if dir == "" {
		return nil, ErrInvalid
	}
	abs, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	return openFS(osFS{}, abs, opts)
}
func openFS(fs filesystem, dir string, opts Options) (db *DB, err error) {
	opts, err = opts.defaults()
	if err != nil {
		return nil, err
	}
	d := &DB{fs: fs, dir: dir, opts: opts, tables: map[uint64]*table{}, mem: map[string]record{}}
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
	m, err := d.readManifest()
	if os.IsNotExist(err) {
		// Only an interrupted initial publication can lack a manifest. Never
		// adopt existing WAL/tables or remove unrelated files as a new DB.
		for _, n := range names {
			if n != "LOCK" && n != "MANIFEST.tmp" {
				return nil, corruption("manifest absent in nonempty directory")
			}
		}
		m = manifest{Version: 1, Generation: 1, NextID: 1, Style: opts.Style}
		if err = d.writeManifest(m, "initialize"); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if m.Style != opts.Style {
		return nil, fmt.Errorf("%w: use the stored compaction style %s", ErrInvalid, m.Style)
	}
	d.manifest = m
	d.seq = m.Checkpoint
	d.nextID = m.NextID
	for _, desc := range m.Tables {
		t, e := d.openTable(desc)
		if e != nil {
			return nil, e
		}
		d.tables[desc.ID] = t
	}
	for _, n := range names {
		if n == "LOCK" || n == "MANIFEST" || n == "MANIFEST.tmp" || n == "WAL" {
			continue
		}
		id, ok := parseTableName(n)
		if !ok {
			return nil, corruption("unrecognized database file")
		}
		if id >= d.nextID {
			if id == ^uint64(0) {
				return nil, ErrLimit
			}
			d.nextID = id + 1
		}
	}
	flags := os.O_RDWR
	if !m.Initialized {
		flags |= os.O_CREATE
	}
	d.wal, err = fs.Open(d.path("WAL"), flags)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, corruption("missing WAL")
		}
		return nil, err
	}
	if !m.Initialized {
		size, e := d.wal.Size()
		if e != nil {
			return nil, e
		}
		if size != 0 {
			return nil, corruption("unfinished initialization has WAL data")
		}
	}
	if err = d.wal.Sync(); err != nil {
		return nil, err
	}
	d.count.Syncs++
	if err = d.syncDir(); err != nil {
		return nil, err
	}
	if !m.Initialized {
		m.Initialized = true
		m.Generation++
		if err = d.writeManifest(m, "initialize"); err != nil {
			return nil, err
		}
		d.manifest = m
	}
	if err = d.recoverWAL(); err != nil {
		return nil, err
	}
	for _, n := range names {
		id, ok := parseTableName(n)
		if n == "MANIFEST.tmp" || ok && (strings.HasPrefix(n, ".tmp-") || d.tables[id] == nil) {
			if e := fs.Remove(d.path(n)); e != nil && !os.IsNotExist(e) {
				return nil, e
			}
		}
	}
	if err = d.syncDir(); err != nil {
		return nil, err
	}
	return d, nil
}
func parseTableName(n string) (uint64, bool) {
	n = strings.TrimPrefix(n, ".tmp-")
	if len(n) != 24 || !strings.HasSuffix(n, ".sst") {
		return 0, false
	}
	id, e := strconv.ParseUint(n[:20], 10, 64)
	return id, e == nil && id > 0 && tableName(id) == n
}
func (d *DB) path(n string) string { return filepath.Join(d.dir, n) }
func (d *DB) hit(p string) error {
	if d.opts.Failpoint != nil {
		return d.opts.Failpoint(p)
	}
	return nil
}
func (d *DB) syncDir() error {
	if e := d.fs.SyncDir(d.dir); e != nil {
		return e
	}
	d.count.Syncs++
	return nil
}
func (d *DB) ready() error {
	if d.closed {
		return ErrClosed
	}
	if d.bad != nil {
		return d.bad
	}
	return nil
}
func (d *DB) poison(e error) error { d.bad = fmt.Errorf("%w: %w", ErrRecovery, e); return d.bad }

func (d *DB) recoverWAL() error {
	size, e := d.wal.Size()
	if e != nil {
		return e
	}
	if size < 0 || size > maxDataBytes+maxRecordBytes+frameHeader {
		return ErrLimit
	}
	offset := int64(0)
	var previous uint64
	for offset < size {
		r, next, e := readRecord(d.wal, offset, size)
		if errors.Is(e, errTail) {
			if e = d.wal.Truncate(offset); e != nil {
				return e
			}
			if e = d.wal.Sync(); e != nil {
				return e
			}
			d.count.Syncs++
			break
		}
		if e != nil {
			return e
		}
		if previous != 0 && (previous == ^uint64(0) || r.seq != previous+1) {
			return corruption("WAL sequence gap")
		}
		previous = r.seq
		if r.seq > d.manifest.Checkpoint {
			if d.seq == ^uint64(0) || r.seq != d.seq+1 {
				return corruption("WAL checkpoint gap")
			}
			d.putMem(r)
			d.seq = r.seq
			d.count.Recovered++
		}
		offset = next
	}
	if len(d.mem) == 0 && size > 0 {
		if e = d.wal.Truncate(0); e != nil {
			return e
		}
		if e = d.wal.Sync(); e != nil {
			return e
		}
		d.count.Syncs++
	}
	_, e = d.wal.Seek(0, io.SeekEnd)
	return e
}
func (d *DB) putMem(r record) {
	k := string(r.key)
	if old, ok := d.mem[k]; ok {
		d.memBytes -= old.size()
	}
	d.mem[k] = r
	d.memBytes += r.size()
}
func (d *DB) Put(key, value []byte) error { return d.write(key, value, false) }
func (d *DB) Delete(key []byte) error     { return d.write(key, nil, true) }
func (d *DB) write(key, value []byte, deleted bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	if !validKey(key) || len(value) > MaxValueBytes {
		return ErrInvalid
	}
	if d.seq == ^uint64(0) {
		return ErrLimit
	}
	s, e := d.wal.Size()
	if e != nil {
		return d.poison(e)
	}
	if s+int64(frameHeader+17+len(key)+len(value)) > maxDataBytes {
		if e = d.flushLocked(); e != nil {
			return d.poison(e)
		}
		if !d.opts.DisableAutoCompaction {
			if e = d.compactAutomatic(); e != nil {
				return d.poison(e)
			}
		}
	}
	r := record{clone(key), clone(value), d.seq + 1, deleted}
	b := encodeRecord(r)
	n, e := writeAll(d.wal, b)
	d.count.WALBytes += uint64(n)
	if e != nil {
		return d.poison(e)
	}
	if e = d.hit("wal.written"); e != nil {
		return d.poison(e)
	}
	if d.opts.Durability == SyncWrites {
		if e = d.wal.Sync(); e != nil {
			return d.poison(e)
		}
		d.count.Syncs++
		if e = d.hit("wal.synced"); e != nil {
			return d.poison(e)
		}
	}
	d.seq = r.seq
	d.putMem(r)
	d.count.LogicalBytes += uint64(len(key) + len(value))
	if d.memBytes >= d.opts.MemtableBytes || len(d.mem) >= maxEntries {
		if e = d.flushLocked(); e != nil {
			return d.poison(e)
		}
		if !d.opts.DisableAutoCompaction {
			if e = d.compactAutomatic(); e != nil {
				return d.poison(e)
			}
		}
	}
	return nil
}
func (d *DB) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	if e := d.wal.Sync(); e != nil {
		return d.poison(e)
	}
	d.count.Syncs++
	if e := d.hit("wal.synced"); e != nil {
		return d.poison(e)
	}
	return nil
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
	if r, ok := d.mem[string(key)]; ok {
		if r.deleted {
			return nil, ErrNotFound
		}
		return clone(r.value), nil
	}
	var best record
	found := false
	for _, desc := range d.manifest.Tables {
		r, ok, e := d.tableGet(d.tables[desc.ID], key)
		if e != nil {
			return nil, e
		}
		// Compaction produces a newer file ID from older versions. Sequence,
		// rather than file creation order, decides which value wins.
		if ok && (!found || r.seq > best.seq) {
			best = r
			found = true
		}
	}
	if !found || best.deleted {
		return nil, ErrNotFound
	}
	return clone(best.value), nil
}
func (d *DB) Scan(start, end []byte, limit int) ([]Pair, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return nil, e
	}
	if len(start) > MaxKeyBytes || len(end) > MaxKeyBytes+1 || limit < 1 || limit > maxEntries || end != nil && bytes.Compare(start, end) > 0 {
		return nil, ErrInvalid
	}
	var result []Pair
	e := d.mergeFrom(d.manifest.Tables, true, start, end, func(r record) bool {
		if bytes.Compare(r.key, start) < 0 {
			return true
		}
		if end != nil && bytes.Compare(r.key, end) >= 0 {
			return false
		}
		if !r.deleted {
			result = append(result, Pair{clone(r.key), clone(r.value)})
		}
		return len(result) < limit
	})
	return result, e
}
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	if e := d.flushLocked(); e != nil {
		return d.poison(e)
	}
	if !d.opts.DisableAutoCompaction {
		if e := d.compactAutomatic(); e != nil {
			return d.poison(e)
		}
	}
	return nil
}
func (d *DB) flushLocked() error {
	if len(d.mem) == 0 {
		return nil
	}
	if e := d.wal.Sync(); e != nil {
		return e
	}
	d.count.Syncs++
	rows := d.sortedMem()
	outputs, e := d.writeOutputs(rows, 0, "flush")
	if e != nil {
		return e
	}
	m := d.manifest
	m.Tables = append(append([]descriptor(nil), m.Tables...), outputs...)
	if len(m.Tables) > maxTables {
		return ErrLimit
	}
	m.Checkpoint = d.seq
	if e = d.publish(m, "flush"); e != nil {
		return e
	}
	d.mem = map[string]record{}
	d.memBytes = 0
	if e = d.wal.Truncate(0); e != nil {
		return e
	}
	if _, e = d.wal.Seek(0, io.SeekStart); e != nil {
		return e
	}
	if e = d.hit("flush.wal.truncated"); e != nil {
		return e
	}
	if e = d.wal.Sync(); e != nil {
		return e
	}
	d.count.Syncs++
	if e = d.hit("flush.wal.synced"); e != nil {
		return e
	}
	d.count.Flushes++
	return nil
}
func (d *DB) sortedMem() []record {
	rows := make([]record, 0, len(d.mem))
	for _, r := range d.mem {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	return rows
}
func (d *DB) Stats() (Stats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return Stats{}, e
	}
	s := Stats{Sequence: d.seq, Checkpoint: d.manifest.Checkpoint, MemtableEntries: len(d.mem), MemtableBytes: d.memBytes, Tables: len(d.manifest.Tables), Levels: map[int]int{}, Counters: d.count}
	for _, t := range d.manifest.Tables {
		s.DiskBytes += t.Size
		s.Levels[t.Level]++
	}
	for _, n := range []string{"WAL", "MANIFEST"} {
		f, e := d.fs.Open(d.path(n), os.O_RDONLY)
		if e != nil {
			return s, e
		}
		v, e := f.Size()
		f.Close()
		if e != nil {
			return s, e
		}
		s.DiskBytes += v
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
	for _, t := range d.tables {
		es = append(es, t.file.Close())
	}
	if d.lock != nil {
		es = append(es, d.lock.Close())
	}
	return errors.Join(es...)
}
