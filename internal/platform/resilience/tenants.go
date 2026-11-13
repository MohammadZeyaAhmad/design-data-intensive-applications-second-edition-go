package resilience

import (
	"context"
	"errors"
	"sync"
)

// Tenants limits each concurrently active tenant before it can compete for the
// shared dependency. Idle entries disappear; client-controlled identities cannot
// grow the registry beyond its fixed bound.
type Tenants struct {
	mu                          sync.Mutex
	entries                     map[string]*tenantEntry
	active, queued, limit, peak int
	rejected                    int64
}
type tenantEntry struct {
	bulkhead *Bulkhead
	refs     int
}
type TenantStats struct {
	Tenants  int   `json:"tenants"`
	Peak     int   `json:"peak_tenants"`
	Limit    int   `json:"limit"`
	Rejected int64 `json:"rejected"`
}

func NewTenants(active, queued, limit int) (*Tenants, error) {
	if _, e := NewBulkhead(active, queued); e != nil {
		return nil, e
	}
	if limit < 1 || limit > 1024 {
		return nil, errors.New("invalid tenant registry limit")
	}
	return &Tenants{entries: map[string]*tenantEntry{}, active: active, queued: queued, limit: limit}, nil
}
func (t *Tenants) Acquire(ctx context.Context, tenant string) (func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	t.mu.Lock()
	entry := t.entries[tenant]
	if entry == nil {
		if len(t.entries) >= t.limit {
			t.rejected++
			t.mu.Unlock()
			return nil, ErrOverloaded
		}
		b, _ := NewBulkhead(t.active, t.queued)
		entry = &tenantEntry{bulkhead: b}
		t.entries[tenant] = entry
		if len(t.entries) > t.peak {
			t.peak = len(t.entries)
		}
	}
	entry.refs++
	t.mu.Unlock()
	forget := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(t.entries, tenant)
		}
	}
	permit, e := entry.bulkhead.Acquire(ctx)
	if e != nil {
		forget()
		t.mu.Lock()
		t.rejected++
		t.mu.Unlock()
		return nil, e
	}
	var once sync.Once
	return func() { once.Do(func() { permit.Release(); forget() }) }, nil
}
func (t *Tenants) Stats() TenantStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return TenantStats{Tenants: len(t.entries), Peak: t.peak, Limit: t.limit, Rejected: t.rejected}
}
