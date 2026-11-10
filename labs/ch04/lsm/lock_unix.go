//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package lsm

import (
	"errors"
	"io"
	"os"
	"syscall"
)

type fileLock struct{ f *os.File }

func (l *fileLock) Close() error {
	return errors.Join(syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN), l.f.Close())
}
func (osFS) Lock(p string) (io.Closer, error) {
	if s, e := os.Lstat(p); e == nil && s.Mode()&os.ModeSymlink != 0 {
		return nil, ErrInvalid
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		if errors.Is(e, syscall.EWOULDBLOCK) || errors.Is(e, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, e
	}
	return &fileLock{f}, nil
}
