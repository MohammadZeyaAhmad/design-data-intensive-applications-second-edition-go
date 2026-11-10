// Package btree implements a bounded page-based B+ tree with full-page redo.
// Individual mutations are synchronous transactions under one writer lock.
package btree

import (
	"fmt"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

const (
	PageSize           = 4096
	MaxKeyBytes        = kv.MaxKeyBytes
	MaxValueBytes      = 256
	MaxPages           = 65536
	maxDirtyPages      = 64
	pageHeader         = 40
	pageCRC            = PageSize - 4
	kindMeta      byte = 1
	kindLeaf      byte = 2
	kindBranch    byte = 3
	kindFree      byte = 4
)

var (
	ErrNotFound = kv.ErrNotFound
	ErrClosed   = kv.ErrClosed
	ErrLocked   = kv.ErrLocked
	ErrInvalid  = kv.ErrInvalid
	ErrCorrupt  = kv.ErrCorrupt
	ErrLimit    = kv.ErrLimit
	ErrRecovery = kv.ErrRecovery
)

type Pair = kv.Pair
type Options struct {
	// MaxKeys is persisted. Zero adopts an existing tree's order, or defaults
	// to eight for a new tree. Even orders four through eight are supported.
	MaxKeys int
	// Failpoint is a lab fault schedule. Returning an error after log mutation
	// requires closing/reopening the poisoned handle. Do not re-enter the DB.
	Failpoint func(string) error
}

func validOrder(n int) bool  { return n >= 4 && n <= 8 && n%2 == 0 }
func validKey(k []byte) bool { return len(k) > 0 && len(k) <= MaxKeyBytes }
func clone(b []byte) []byte  { return append([]byte(nil), b...) }
func corrupt(message string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(message, args...))
}

type metadata struct {
	ID                                [16]byte
	Generation                        uint64
	Root, Next, Free, FreeCount, Keys uint64
	Order, Height                     int
}
type page struct {
	id, lsn, next uint64
	kind          byte
	keys, values  [][]byte
	children      []uint64
}
type Counters struct {
	LogicalBytes uint64 `json:"logical_bytes"`
	WALBytes     uint64 `json:"wal_bytes_written"`
	DataBytes    uint64 `json:"data_bytes_written"`
	ReadBytes    uint64 `json:"page_bytes_read"`
	Syncs        uint64 `json:"sync_calls"`
	Splits       uint64 `json:"splits"`
	Merges       uint64 `json:"merges"`
	Borrows      uint64 `json:"borrows"`
	FreeReuses   uint64 `json:"free_page_reuses"`
	Recovered    uint64 `json:"recovered_transactions"`
}
type Stats struct {
	Generation     uint64   `json:"generation"`
	Keys           uint64   `json:"keys"`
	Height         int      `json:"height"`
	MaxKeys        int      `json:"max_keys"`
	AllocatedPages uint64   `json:"allocated_pages"`
	FreePages      uint64   `json:"free_pages"`
	DiskBytes      int64    `json:"disk_bytes"`
	Counters       Counters `json:"counters"`
}
type CheckReport struct {
	Keys        uint64 `json:"keys"`
	Height      int    `json:"height"`
	TreePages   uint64 `json:"tree_pages"`
	LeafPages   uint64 `json:"leaf_pages"`
	BranchPages uint64 `json:"branch_pages"`
	FreePages   uint64 `json:"free_pages"`
}
