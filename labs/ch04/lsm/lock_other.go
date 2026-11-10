//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package lsm

import "io"

func (osFS) Lock(string) (io.Closer, error) { return nil, ErrUnsupported }
