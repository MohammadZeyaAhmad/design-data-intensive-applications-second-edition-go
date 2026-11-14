//go:build !unix

package objectstorage

import (
	"context"
	"errors"
	"io"
)

func lockFile(context.Context, string) (io.Closer, error) {
	return nil, errors.New("file object backend requires Unix process locks")
}
