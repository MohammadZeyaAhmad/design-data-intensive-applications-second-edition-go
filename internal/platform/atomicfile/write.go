// Package atomicfile publishes complete immutable local artifacts.
package atomicfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
)

var ErrExists = errors.New("immutable artifact already contains different bytes")

func WriteOnce(path string, content []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".atlas-publish-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(content); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Link(f.Name(), path); e != nil {
		if !errors.Is(e, os.ErrExist) {
			return e
		}
		stat, e := os.Lstat(path)
		if e != nil {
			return e
		}
		if !stat.Mode().IsRegular() || stat.Size() != int64(len(content)) {
			return ErrExists
		}
		old, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if !bytes.Equal(old, content) {
			return ErrExists
		}
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
