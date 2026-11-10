//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package diskio

import (
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"io"
)

func (OS) Lock(string) (io.Closer, error) { return nil, kv.ErrUnsupported }
