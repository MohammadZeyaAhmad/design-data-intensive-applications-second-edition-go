// Package objectstorage separates immutable data from conditional publication.
// It is a bounded warehouse read-model lab, never Inventory write authority.
package objectstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"sync"
)

const MaxObjectBytes = 1 << 20
const MaxObjects = 4096

var (
	ErrNotFound  = errors.New("object not found")
	ErrConflict  = errors.New("conditional publication conflict")
	ErrUncertain = errors.New("write outcome uncertain; reconcile the same intent")
	ErrIntent    = errors.New("intent already has a different payload")
	ErrCorrupt   = errors.New("incomplete or corrupt object generation")
	ErrInvalid   = errors.New("invalid object-storage argument")
	ErrLimit     = errors.New("bounded object-storage limit exceeded")
	ErrAccess    = errors.New("object-store access denied")
)

type Object struct {
	Body    []byte
	Version string
}
type Condition struct {
	Absent  bool
	Version string
}

func (c Condition) valid() bool {
	return c.Absent != (c.Version != "") && len(c.Version) <= 256 && !strings.ContainsAny(c.Version, "\r\n")
}

// Versions are opaque CAS tokens, not hashes, generations, or S3 VersionIds.
// Get/Put require strong per-key visibility. List is only an inventory aid.
// Delete is a maintenance operation used by owned-prefix integration cleanup;
// the publication engine never deletes live/history/snapshot objects.
type Store interface {
	Get(context.Context, string) (Object, error)
	Put(context.Context, string, []byte, Condition) (string, error)
	List(context.Context, string) ([]string, error)
	Delete(context.Context, string) error
}

var objectKey = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9/._-]{0,255}$`)

func validKey(key string) bool {
	if !objectKey.MatchString(key) {
		return false
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func sum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type Counts struct {
	Gets       uint64 `json:"get_attempts"`
	Puts       uint64 `json:"put_attempts"`
	Lists      uint64 `json:"list_attempts"`
	Deletes    uint64 `json:"delete_attempts"`
	Conflicts  uint64 `json:"conditional_conflicts"`
	ReadBytes  uint64 `json:"response_body_bytes"`
	WriteBytes uint64 `json:"attempted_put_body_bytes"`
}

// Meter counts logical API attempts including rejected writes, without money
// estimates, provider storage charges, transport overhead or internal retries.
type Meter struct {
	Store  Store
	mu     sync.Mutex
	counts Counts
}

func (m *Meter) Snapshot() Counts { m.mu.Lock(); defer m.mu.Unlock(); return m.counts }
func (m *Meter) Get(ctx context.Context, key string) (Object, error) {
	o, e := m.Store.Get(ctx, key)
	m.mu.Lock()
	m.counts.Gets++
	m.counts.ReadBytes += uint64(len(o.Body))
	m.mu.Unlock()
	return o, e
}
func (m *Meter) Put(ctx context.Context, key string, body []byte, c Condition) (string, error) {
	v, e := m.Store.Put(ctx, key, body, c)
	m.mu.Lock()
	m.counts.Puts++
	m.counts.WriteBytes += uint64(len(body))
	if errors.Is(e, ErrConflict) {
		m.counts.Conflicts++
	}
	m.mu.Unlock()
	return v, e
}
func (m *Meter) List(ctx context.Context, prefix string) ([]string, error) {
	keys, e := m.Store.List(ctx, prefix)
	m.mu.Lock()
	m.counts.Lists++
	m.mu.Unlock()
	return keys, e
}
func (m *Meter) Delete(ctx context.Context, key string) error {
	e := m.Store.Delete(ctx, key)
	m.mu.Lock()
	m.counts.Deletes++
	m.mu.Unlock()
	return e
}
