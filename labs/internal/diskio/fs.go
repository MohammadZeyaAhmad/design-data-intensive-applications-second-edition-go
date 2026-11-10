// Package diskio supplies the explicit file/lock contract used by storage labs.
package diskio

import (
	"io"
	"os"
	"path/filepath"
)

type File interface {
	io.WriterAt
	io.ReaderAt
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
	Size() (int64, error)
	Close() error
}
type FS interface {
	MkdirAll(string) error
	List(string) ([]string, error)
	Open(string, int) (File, error)
	Rename(string, string) error
	Remove(string) error
	SyncDir(string) error
	Lock(string) (io.Closer, error)
}
type OS struct{}
type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	s, e := f.Stat()
	if e != nil {
		return 0, e
	}
	return s.Size(), nil
}
func (OS) Open(p string, flags int) (File, error) {
	f, e := os.OpenFile(p, flags, 0600)
	if e != nil {
		return nil, e
	}
	return osFile{f}, nil
}
func (OS) List(p string) ([]string, error) {
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
func (OS) Rename(a, b string) error { return os.Rename(a, b) }
func (OS) Remove(p string) error    { return os.Remove(p) }
func (OS) SyncDir(p string) error {
	f, e := os.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (f OS) MkdirAll(p string) error {
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
func WriteAll(f io.Writer, b []byte) (int, error) {
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

func WriteAllAt(f io.WriterAt, b []byte, offset int64) (int, error) {
	total := 0
	for len(b) > 0 {
		n, e := f.WriteAt(b, offset)
		total += n
		b = b[n:]
		offset += int64(n)
		if e != nil {
			return total, e
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
