// Package diskmodel is a deterministic filesystem fault model for lab tests.
// It models one durable parent directory, file fsync and atomic rename.
package diskmodel

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskio"
)

// FS separates inode contents from directory-entry durability. A crash
// restores only fsynced data/names, with optional early persistence/reordering.
// It models the declared fsync/atomic-rename contract, not a particular SSD.
type node struct{ live, stable []byte }
type FS struct {
	nodes                                    map[int]*node
	live, stable                             map[string]int
	next                                     int
	locked                                   bool
	WriteThrough, EagerManifest, EagerRemove bool
	Chunk                                    int
	Active                                   bool
	Cut, Count                               int
}
type CrashEvent struct{}

func New() *FS {
	return &FS{nodes: map[int]*node{}, live: map[string]int{}, stable: map[string]int{}, next: 1}
}
func (f *FS) event() {
	if !f.Active {
		return
	}
	f.Count++
	if f.Count == f.Cut {
		panic(CrashEvent{})
	}
}
func (f *FS) MkdirAll(string) error { return nil } // test starts with a durable directory
func (f *FS) List(dir string) ([]string, error) {
	var r []string
	for p := range f.live {
		if filepath.Dir(p) == dir {
			r = append(r, filepath.Base(p))
		}
	}
	sort.Strings(r)
	return r, nil
}
func (f *FS) Open(path string, flags int) (diskio.File, error) {
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
		f.nodes[id] = &node{}
		f.live[path] = id
		f.event()
	}
	if flags&os.O_TRUNC != 0 {
		f.nodes[id].live = nil
		if f.WriteThrough {
			f.nodes[id].stable = nil
		}
		f.event()
	}
	return &crashFile{fs: f, id: id, write: flags&(os.O_WRONLY|os.O_RDWR) != 0}, nil
}
func (f *FS) Rename(a, b string) error {
	id, ok := f.live[a]
	if !ok {
		return os.ErrNotExist
	}
	delete(f.live, a)
	f.live[b] = id
	if f.EagerManifest && (filepath.Base(b) == "MANIFEST" || filepath.Base(b) == "CONTROL") {
		delete(f.stable, a)
		f.stable[b] = id
	}
	f.event()
	return nil
}
func (f *FS) Remove(p string) error {
	if _, ok := f.live[p]; !ok {
		return os.ErrNotExist
	}
	delete(f.live, p)
	if f.EagerRemove {
		delete(f.stable, p)
	}
	f.event()
	return nil
}
func (f *FS) SyncDir(string) error { f.stable = copyNames(f.live); f.event(); return nil }

type crashLock struct{ fs *FS }

func (l *crashLock) Close() error { l.fs.locked = false; return nil }
func (f *FS) Lock(p string) (io.Closer, error) {
	if f.locked {
		return nil, kv.ErrLocked
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
func (f *FS) Crash() {
	f.live = copyNames(f.stable)
	for _, n := range f.nodes {
		n.live = clone(n.stable)
	}
	f.locked = false
	f.Active = false
	f.Cut = 0
}
func (f *FS) Snapshot() *FS {
	r := New()
	r.next = f.next
	r.live = copyNames(f.live)
	r.stable = copyNames(f.stable)
	for k, n := range f.nodes {
		r.nodes[k] = &node{clone(n.live), clone(n.stable)}
	}
	r.WriteThrough = f.WriteThrough
	r.EagerManifest = f.EagerManifest
	r.EagerRemove = f.EagerRemove
	r.Chunk = f.Chunk
	return r
}

type crashFile struct {
	fs            *FS
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
	if f.fs.Chunk > 0 && len(b) > f.fs.Chunk {
		b = b[:f.fs.Chunk]
	}
	node := f.fs.nodes[f.id]
	end := int(f.pos) + len(b)
	if end > len(node.live) {
		node.live = append(node.live, make([]byte, end-len(node.live))...)
	}
	copy(node.live[int(f.pos):], b)
	f.pos = int64(end)
	if f.fs.WriteThrough {
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
		return 0, kv.ErrInvalid
	}
	if p < 0 {
		return 0, kv.ErrInvalid
	}
	f.pos = p
	return p, nil
}
func (f *crashFile) Truncate(size int64) error {
	if size < 0 || !f.write {
		return kv.ErrInvalid
	}
	n := f.fs.nodes[f.id]
	if size <= int64(len(n.live)) {
		n.live = n.live[:size]
	} else {
		n.live = append(n.live, make([]byte, int(size)-len(n.live))...)
	}
	if f.fs.WriteThrough {
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

func clone(b []byte) []byte { return append([]byte(nil), b...) }
func (f *crashFile) WriteAt(b []byte, offset int64) (int, error) {
	old := f.pos
	f.pos = offset
	n, e := f.Write(b)
	f.pos = old
	return n, e
}
