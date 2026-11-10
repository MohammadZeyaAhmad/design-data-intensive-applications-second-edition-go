package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// crashFS separates inode contents from directory-entry durability. A crash
// restores only fsynced data/names, with optional early persistence/reordering.
// It models the declared fsync/atomic-rename contract, not a particular SSD.
type crashNode struct{ live, stable []byte }
type crashFS struct {
	nodes                                    map[int]*crashNode
	live, stable                             map[string]int
	next                                     int
	locked                                   bool
	writeThrough, eagerManifest, eagerRemove bool
	chunk                                    int
	active                                   bool
	cut, count                               int
}
type powerLoss struct{}

func newCrashFS() *crashFS {
	return &crashFS{nodes: map[int]*crashNode{}, live: map[string]int{}, stable: map[string]int{}, next: 1}
}
func (f *crashFS) event() {
	if !f.active {
		return
	}
	f.count++
	if f.count == f.cut {
		panic(powerLoss{})
	}
}
func (f *crashFS) MkdirAll(string) error { return nil } // test starts with a durable directory
func (f *crashFS) List(dir string) ([]string, error) {
	var r []string
	for p := range f.live {
		if filepath.Dir(p) == dir {
			r = append(r, filepath.Base(p))
		}
	}
	sort.Strings(r)
	return r, nil
}
func (f *crashFS) Open(path string, flags int) (dataFile, error) {
	id, ok := f.live[path]
	if ok && flags&os.O_EXCL != 0 {
		return nil, os.ErrExist
	}
	if !ok {
		if flags&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		id = f.next
		f.next++
		f.nodes[id] = &crashNode{}
		f.live[path] = id
		f.event()
	}
	if flags&os.O_TRUNC != 0 {
		f.nodes[id].live = nil
		if f.writeThrough {
			f.nodes[id].stable = nil
		}
		f.event()
	}
	return &crashFile{fs: f, id: id, write: flags&(os.O_WRONLY|os.O_RDWR) != 0}, nil
}
func (f *crashFS) Rename(a, b string) error {
	id, ok := f.live[a]
	if !ok {
		return os.ErrNotExist
	}
	delete(f.live, a)
	f.live[b] = id
	if f.eagerManifest && filepath.Base(b) == "MANIFEST" {
		delete(f.stable, a)
		f.stable[b] = id
	}
	f.event()
	return nil
}
func (f *crashFS) Remove(p string) error {
	if _, ok := f.live[p]; !ok {
		return os.ErrNotExist
	}
	delete(f.live, p)
	if f.eagerRemove {
		delete(f.stable, p)
	}
	f.event()
	return nil
}
func (f *crashFS) SyncDir(string) error { f.stable = copyNames(f.live); f.event(); return nil }

type crashLock struct{ fs *crashFS }

func (l *crashLock) Close() error { l.fs.locked = false; return nil }
func (f *crashFS) Lock(p string) (io.Closer, error) {
	if f.locked {
		return nil, ErrLocked
	}
	h, e := f.Open(p, os.O_CREATE|os.O_RDWR)
	if e != nil {
		return nil, e
	}
	h.Close()
	f.locked = true
	return &crashLock{f}, nil
}
func copyNames(m map[string]int) map[string]int {
	r := map[string]int{}
	for k, v := range m {
		r[k] = v
	}
	return r
}
func (f *crashFS) crash() {
	f.live = copyNames(f.stable)
	for _, n := range f.nodes {
		n.live = clone(n.stable)
	}
	f.locked = false
	f.active = false
	f.cut = 0
}
func (f *crashFS) snapshot() *crashFS {
	r := newCrashFS()
	r.next = f.next
	r.live = copyNames(f.live)
	r.stable = copyNames(f.stable)
	for k, n := range f.nodes {
		r.nodes[k] = &crashNode{clone(n.live), clone(n.stable)}
	}
	r.writeThrough = f.writeThrough
	r.eagerManifest = f.eagerManifest
	r.eagerRemove = f.eagerRemove
	r.chunk = f.chunk
	return r
}

type crashFile struct {
	fs            *crashFS
	id            int
	pos           int64
	write, closed bool
}

func (f *crashFile) ReadAt(b []byte, offset int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return bytes.NewReader(f.fs.nodes[f.id].live).ReadAt(b, offset)
}
func (f *crashFile) Write(b []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if !f.write {
		return 0, os.ErrPermission
	}
	if f.fs.chunk > 0 && len(b) > f.fs.chunk {
		b = b[:f.fs.chunk]
	}
	node := f.fs.nodes[f.id]
	end := int(f.pos) + len(b)
	if end > len(node.live) {
		node.live = append(node.live, make([]byte, end-len(node.live))...)
	}
	copy(node.live[int(f.pos):], b)
	f.pos = int64(end)
	if f.fs.writeThrough {
		node.stable = clone(node.live)
	}
	f.fs.event()
	return len(b), nil
}
func (f *crashFile) Seek(offset int64, whence int) (int64, error) {
	var p int64
	switch whence {
	case io.SeekStart:
		p = offset
	case io.SeekCurrent:
		p = f.pos + offset
	case io.SeekEnd:
		p = int64(len(f.fs.nodes[f.id].live)) + offset
	default:
		return 0, ErrInvalid
	}
	if p < 0 {
		return 0, ErrInvalid
	}
	f.pos = p
	return p, nil
}
func (f *crashFile) Truncate(size int64) error {
	if size < 0 || !f.write {
		return ErrInvalid
	}
	n := f.fs.nodes[f.id]
	if size <= int64(len(n.live)) {
		n.live = n.live[:size]
	} else {
		n.live = append(n.live, make([]byte, int(size)-len(n.live))...)
	}
	if f.fs.writeThrough {
		n.stable = clone(n.live)
	}
	f.fs.event()
	return nil
}
func (f *crashFile) Sync() error {
	f.fs.nodes[f.id].stable = clone(f.fs.nodes[f.id].live)
	f.fs.event()
	return nil
}
func (f *crashFile) Size() (int64, error) { return int64(len(f.fs.nodes[f.id].live)), nil }
func (f *crashFile) Close() error         { f.closed = true; return nil }

var powerOperations = []record{
	{key: []byte("a"), value: []byte("new")},
	{key: []byte("b"), deleted: true},
	{key: []byte("d"), value: []byte("last")},
	{key: []byte("a"), deleted: true},
}

func powerOracle(prefix int) map[string]string {
	r := map[string]string{"a": "base", "b": "base", "c": "base"}
	for _, op := range powerOperations[:prefix] {
		if op.deleted {
			delete(r, string(op.key))
		} else {
			r[string(op.key)] = string(op.value)
		}
	}
	return r
}

type powerProgress struct{ issued, durable int }

func runPowerSchedule(d *DB, p *powerProgress) error {
	apply := func(op record) error {
		p.issued++
		var e error
		if op.deleted {
			e = d.Delete(op.key)
		} else {
			e = d.Put(op.key, op.value)
		}
		if e == nil && d.opts.Durability == SyncWrites {
			p.durable = p.issued
		}
		return e
	}
	if e := apply(powerOperations[0]); e != nil {
		return e
	}
	if e := apply(powerOperations[1]); e != nil {
		return e
	}
	if e := d.Flush(); e != nil {
		return e
	}
	p.durable = p.issued
	if e := apply(powerOperations[2]); e != nil {
		return e
	}
	if e := d.Sync(); e != nil {
		return e
	}
	p.durable = p.issued
	if e := d.Compact(); e != nil {
		return e
	}
	if e := apply(powerOperations[3]); e != nil {
		return e
	}
	if e := d.Flush(); e != nil {
		return e
	}
	p.durable = p.issued
	return d.Compact()
}
func interruptPower(f *crashFS, d *DB, p *powerProgress) (crashed bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(powerLoss); !ok {
				panic(v)
			}
			crashed = true
		}
	}()
	err = runPowerSchedule(d, p)
	return
}
func TestPowerLossAtEveryFilesystemMutation(t *testing.T) {
	variants := []struct {
		data, manifest, remove bool
		chunk                  int
	}{
		{}, {true, true, false, 0}, {true, false, true, 0}, {false, true, true, 17},
	}
	for _, style := range []Style{Tiered, Leveled} {
		for _, mode := range []Durability{SyncWrites, AsyncWrites} {
			for v, variant := range variants {
				t.Run(fmt.Sprintf("%s/%s/schedule-%d", style, mode, v), func(t *testing.T) {
					base := newCrashFS()
					o := Options{Style: style, Durability: mode, DisableAutoCompaction: true, BlockBytes: 1024, TargetTableBytes: 2048}
					d, e := openFS(base, "db", o)
					must(t, e)
					for _, k := range []string{"a", "b"} {
						must(t, d.Put([]byte(k), []byte("base")))
					}
					must(t, d.Flush())
					must(t, d.Put([]byte("c"), []byte("base")))
					must(t, d.Flush())
					must(t, d.Close())
					base.writeThrough = variant.data
					base.eagerManifest = variant.manifest
					base.eagerRemove = variant.remove
					base.chunk = variant.chunk
					countFS := base.snapshot()
					d, e = openFS(countFS, "db", o)
					must(t, e)
					countFS.active = true
					must(t, runPowerSchedule(d, &powerProgress{}))
					operations := countFS.count
					must(t, d.Close())
					if operations < 20 {
						t.Fatal("schedule omitted publication boundaries", operations)
					}
					for cut := 1; cut <= operations; cut++ {
						fs := base.snapshot()
						db, e := openFS(fs, "db", o)
						must(t, e)
						fs.cut = cut
						fs.active = true
						progress := &powerProgress{}
						crashed, e := interruptPower(fs, db, progress)
						if e != nil || !crashed {
							t.Fatalf("cut %d/%d: %v crash=%v", cut, operations, e, crashed)
						}
						fs.crash()
						recovered, e := openFS(fs, "db", o)
						if e != nil {
							t.Fatalf("cut %d/%d: reopen %v", cut, operations, e)
						}
						s, e := recovered.Stats()
						must(t, e)
						prefix := int(s.Sequence) - 3
						if prefix < progress.durable || prefix > progress.issued {
							t.Fatalf("cut %d: recovered prefix %d outside [%d,%d]", cut, prefix, progress.durable, progress.issued)
						}
						got, e := recovered.Scan(nil, nil, maxEntries)
						must(t, e)
						if !reflect.DeepEqual(got, sortedPairs(powerOracle(prefix))) {
							t.Fatalf("cut %d: values differ at prefix %d", cut, prefix)
						}
						must(t, recovered.Close())
					}
					t.Logf("%d filesystem crash positions checked", operations)
				})
			}
		}
	}
}

func TestInitializationNeverAdoptsForeignData(t *testing.T) {
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0600))
	if d, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
		if d != nil {
			d.Close()
		}
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "notes.txt"))
	must(t, e)
	if string(b) != "keep" {
		t.Fatal("unrelated data changed")
	}
}
