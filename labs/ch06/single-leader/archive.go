package singleleader

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var walName = regexp.MustCompile(`^(?:[0-9A-F]{24}(?:\.partial|\.[0-9A-F]{8}\.backup)?|[0-9A-F]{8}\.history)$`)

// Archive publishes a complete fsynced WAL object without replacing an existing
// object. PostgreSQL retries archive_command; identical retries are successful.
func Archive(dir, source, name string) error {
	if !walName.MatchString(name) {
		return errors.New("invalid WAL archive name")
	}
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	st, err := src.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() > 64<<20 {
		return errors.New("invalid WAL source")
	}
	tmp, err := os.CreateTemp(dir, ".wal-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err = io.Copy(tmp, src); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	dest := filepath.Join(dir, name)
	if err = os.Link(tmp.Name(), dest); err != nil {
		if !os.IsExist(err) {
			return err
		}
		prior, e := os.ReadFile(dest)
		if e != nil {
			return e
		}
		if _, e = tmp.Seek(0, 0); e != nil {
			return e
		}
		current, e := io.ReadAll(tmp)
		if e != nil {
			return e
		}
		if !bytes.Equal(prior, current) {
			return errors.New("WAL archive identity conflict")
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func Restore(dir, name, destination string) error {
	if !walName.MatchString(name) {
		return errors.New("invalid WAL restore name")
	}
	src, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	defer src.Close()
	st, err := src.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() > 64<<20 {
		return errors.New("invalid WAL object")
	}
	tmp, err := os.CreateTemp(filepath.Dir(destination), ".restore-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err = io.Copy(tmp, src); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), destination); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
