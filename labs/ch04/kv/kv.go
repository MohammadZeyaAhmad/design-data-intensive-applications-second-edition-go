// Package kv defines the small comparison contract shared by storage labs.
package kv

import "errors"

const MaxKeyBytes = 128
const MaxValueBytes = 1 << 20
const MaxScan = 65536

var (
	ErrNotFound    = errors.New("key not found")
	ErrClosed      = errors.New("database closed")
	ErrLocked      = errors.New("database already open")
	ErrCorrupt     = errors.New("corrupt storage")
	ErrInvalid     = errors.New("invalid storage argument")
	ErrLimit       = errors.New("storage lab limit exceeded")
	ErrRecovery    = errors.New("write outcome uncertain; close and reopen database")
	ErrUnsupported = errors.New("filesystem locking unsupported on this platform")
)

type Pair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
type Store interface {
	Put([]byte, []byte) error
	Delete([]byte) error
	Get([]byte) ([]byte, error)
	Scan([]byte, []byte, int) ([]Pair, error)
	Close() error
}

func PrefixEnd(prefix []byte) []byte {
	b := append([]byte(nil), prefix...)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 255 {
			b[i]++
			return b[:i+1]
		}
	}
	return nil
}
