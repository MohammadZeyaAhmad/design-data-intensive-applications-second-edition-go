// Package lsm implements a bounded, single-writer log-structured storage lab.
// It is deliberately independent of the application's authoritative databases.
package lsm

import (
	"errors"
	"fmt"
)

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

const (
	MaxKeyBytes    = 128
	MaxValueBytes  = 1 << 20
	maxRecordBytes = 17 + MaxKeyBytes + MaxValueBytes
	maxDataBytes   = 16 << 20
	maxIndexBytes  = 4 << 20
	maxFileBytes   = maxDataBytes + maxIndexBytes + 32
	maxEntries     = 65536
	maxTables      = 128
	maxLevel       = 3
	frameHeader    = 16
)

type Style string

const (
	Leveled Style = "leveled"
	Tiered  Style = "tiered"
)

type Durability string

const (
	SyncWrites  Durability = "sync"
	AsyncWrites Durability = "async"
)

type Options struct {
	MemtableBytes         int
	BlockBytes            int
	TargetTableBytes      int
	Fanout                int
	Style                 Style
	Durability            Durability
	DisableAutoCompaction bool
	// Failpoint is for explicit lab fault schedules. Returning an error poisons
	// the handle; another operation requires reopening. Never re-enter the DB.
	Failpoint func(string) error
}

func (o Options) defaults() (Options, error) {
	if o.MemtableBytes == 0 {
		o.MemtableBytes = 64 << 10
	}
	if o.BlockBytes == 0 {
		o.BlockBytes = 4 << 10
	}
	if o.TargetTableBytes == 0 {
		o.TargetTableBytes = 256 << 10
	}
	if o.Fanout == 0 {
		o.Fanout = 4
	}
	if o.Style == "" {
		o.Style = Leveled
	}
	if o.Durability == "" {
		o.Durability = SyncWrites
	}
	if o.MemtableBytes < 256 || o.MemtableBytes > maxDataBytes || o.BlockBytes < 1024 || o.BlockBytes > 64<<10 || o.TargetTableBytes < o.BlockBytes || o.TargetTableBytes > maxDataBytes || o.Fanout < 2 || o.Fanout > 16 || (o.Style != Leveled && o.Style != Tiered) || (o.Durability != SyncWrites && o.Durability != AsyncWrites) {
		return o, ErrInvalid
	}
	return o, nil
}

type Pair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
type Counters struct {
	LogicalBytes  uint64 `json:"logical_bytes"`
	WALBytes      uint64 `json:"wal_bytes_written"`
	TableBytes    uint64 `json:"table_bytes_written"`
	ManifestBytes uint64 `json:"manifest_bytes_written"`
	ReadBytes     uint64 `json:"table_bytes_read"`
	VerifiedBytes uint64 `json:"table_bytes_verified"`
	BlockReads    uint64 `json:"block_reads"`
	BloomChecks   uint64 `json:"bloom_checks"`
	BloomSkips    uint64 `json:"bloom_skips"`
	Syncs         uint64 `json:"sync_calls"`
	Flushes       uint64 `json:"flushes"`
	Compactions   uint64 `json:"compactions"`
	Recovered     uint64 `json:"recovered_wal_records"`
}
type Stats struct {
	Sequence        uint64      `json:"sequence"`
	Checkpoint      uint64      `json:"checkpoint"`
	MemtableEntries int         `json:"memtable_entries"`
	MemtableBytes   int         `json:"memtable_bytes"`
	Tables          int         `json:"tables"`
	Levels          map[int]int `json:"levels"`
	DiskBytes       int64       `json:"disk_bytes"`
	Counters        Counters    `json:"counters"`
}
type record struct {
	key, value []byte
	seq        uint64
	deleted    bool
}

func (r record) size() int   { return frameHeader + 17 + len(r.key) + len(r.value) }
func clone(b []byte) []byte  { return append([]byte(nil), b...) }
func validKey(k []byte) bool { return len(k) > 0 && len(k) <= MaxKeyBytes }
func corruption(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// PrefixEnd returns the exclusive bound for a byte prefix; nil means unbounded.
func PrefixEnd(prefix []byte) []byte {
	b := clone(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 255 {
			b[i]++
			return b[:i+1]
		}
	}
	return nil
}
