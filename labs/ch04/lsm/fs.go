package lsm

import (
	"io"
	"os"
	"path/filepath"
)

type dataFile interface {
	io.ReaderAt
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
	Size() (int64, error)
	Close() error
}
type filesystem interface {
	MkdirAll(string) error
	List(string) ([]string, error)
	Open(string, int) (dataFile, error)
	Rename(string, string) error
	Remove(string) error
	SyncDir(string) error
	Lock(string) (io.Closer, error)
}
type osFS struct{}
type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	s, e := f.Stat()
	if e != nil {
		return 0, e
	}
	return s.Size(), nil
}
func (osFS) Open(p string, flags int) (dataFile, error) {
	f, e := os.OpenFile(p, flags, 0600)
	if e != nil {
		return nil, e
	}
	return osFile{f}, nil
}
func (osFS) List(p string) ([]string, error) {
	es, e := os.ReadDir(p)
	if e != nil {
		return nil, e
	}
	ns := make([]string, len(es))
	for i, x := range es {
		ns[i] = x.Name()
	}
	return ns, nil
}
func (osFS) Rename(a, b string) error { return os.Rename(a, b) }
func (osFS) Remove(p string) error    { return os.Remove(p) }
func (osFS) SyncDir(p string) error {
	f, e := os.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (f osFS) MkdirAll(p string) error {
	// Persist each new directory entry in its parent before acknowledging data.
	var created []string
	for d := p; ; d = filepath.Dir(d) {
		_, e := os.Stat(d)
		if e == nil {
			break
		}
		if !os.IsNotExist(e) {
			return e
		}
		created = append(created, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	if e := os.MkdirAll(p, 0700); e != nil {
		return e
	}
	for i := len(created) - 1; i >= 0; i-- {
		if e := f.SyncDir(filepath.Dir(created[i])); e != nil {
			return e
		}
	}
	// Also persist the DB entry when the caller just created the directory.
	return f.SyncDir(filepath.Dir(p))
}
func writeAll(f io.Writer, b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		n, e := f.Write(b)
		total += n
		b = b[n:]
		if e != nil {
			return total, e
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
