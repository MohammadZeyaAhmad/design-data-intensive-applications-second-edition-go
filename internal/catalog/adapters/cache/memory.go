package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

type memoryEntry struct {
	key             string
	floor, version  int64
	payload         []byte
	expires, retire time.Time
}
type Memory struct {
	mu        sync.Mutex
	capacity  int
	entries   map[string]*list.Element
	lru       *list.List
	now       func() time.Time
	evictions int64
}

func NewMemory(capacity int) (*Memory, error) {
	if capacity < 1 || capacity > 100000 {
		return nil, errors.New("invalid memory cache capacity")
	}
	return &Memory{capacity: capacity, entries: map[string]*list.Element{}, lru: list.New(), now: time.Now}, nil
}
func (m *Memory) find(key string, create bool) *memoryEntry {
	if element := m.entries[key]; element != nil {
		entry := element.Value.(*memoryEntry)
		if !m.now().Before(entry.retire) {
			m.lru.Remove(element)
			delete(m.entries, key)
		} else {
			m.lru.MoveToFront(element)
			return entry
		}
	}
	if !create {
		return nil
	}
	if len(m.entries) >= m.capacity {
		element := m.lru.Back()
		delete(m.entries, element.Value.(*memoryEntry).key)
		m.lru.Remove(element)
		m.evictions++
	}
	entry := &memoryEntry{key: key}
	m.entries[key] = m.lru.PushFront(entry)
	return entry
}
func (m *Memory) Get(ctx context.Context, key string) (Snapshot, error) {
	if e := ctx.Err(); e != nil {
		return Snapshot{}, e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.find(key, false)
	if entry == nil {
		return Snapshot{}, nil
	}
	snapshot := Snapshot{Floor: entry.floor, Version: entry.version}
	if m.now().Before(entry.expires) {
		snapshot.Payload = append([]byte(nil), entry.payload...)
		snapshot.Remaining = entry.expires.Sub(m.now())
	}
	return snapshot, nil
}
func (m *Memory) Put(ctx context.Context, key string, version int64, payload []byte, ttl, retention time.Duration) (bool, error) {
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if version < 0 || len(payload) > MaxValueBytes || ttl <= 0 || retention < ttl {
		return false, ErrCache
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.find(key, true)
	if version < entry.floor || version < entry.version {
		return false, nil
	}
	entry.version = version
	entry.payload = append([]byte(nil), payload...)
	entry.expires = m.now().Add(ttl)
	entry.retire = m.now().Add(retention)
	return true, nil
}
func (m *Memory) Invalidate(ctx context.Context, key string, version int64, retention time.Duration) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if version < 0 || retention <= 0 {
		return ErrCache
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.find(key, true)
	if version > entry.floor {
		entry.floor = version
	}
	if entry.version < entry.floor {
		entry.payload = nil
		entry.expires = time.Time{}
	}
	entry.retire = m.now().Add(retention)
	return nil
}
func (m *Memory) Discard(ctx context.Context, key string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry := m.find(key, false); entry != nil {
		entry.payload = nil
		entry.expires = time.Time{}
	}
	return nil
}
func (m *Memory) Size() (int, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries), m.evictions
}
