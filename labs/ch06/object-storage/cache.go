package objectstorage

import (
	"container/list"
	"context"
	"sync"
)

type CacheStats struct {
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
	Bytes     int    `json:"resident_bytes"`
	Entries   int    `json:"entries"`
}
type cacheEntry struct {
	key  string
	body []byte
}
type Cache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*list.Element
	order    *list.List
	stats    CacheStats
}

func NewCache(bytes int) (*Cache, error) {
	if bytes < 1024 || bytes > 16*MaxObjectBytes {
		return nil, ErrInvalid
	}
	return &Cache{capacity: bytes, entries: map[string]*list.Element{}, order: list.New()}, nil
}
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Entries = len(c.entries)
	return s
}

// Only content-addressed bodies enter the cache, after full hash validation.
// Current-generation pointers are always fetched from the underlying store.
func (c *Cache) fetch(ctx context.Context, s Store, key, digest string) ([]byte, error) {
	if c == nil {
		o, err := s.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if sum(o.Body) != digest {
			return nil, ErrCorrupt
		}
		return o.Body, nil
	}
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e)
		c.stats.Hits++
		b := append([]byte(nil), e.Value.(cacheEntry).body...)
		c.mu.Unlock()
		return b, nil
	}
	c.stats.Misses++
	c.mu.Unlock()
	o, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if sum(o.Body) != digest {
		return nil, ErrCorrupt
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[key]; !exists && len(o.Body) <= c.capacity {
		for c.stats.Bytes+len(o.Body) > c.capacity {
			old := c.order.Back()
			item := old.Value.(cacheEntry)
			c.stats.Bytes -= len(item.body)
			delete(c.entries, item.key)
			c.order.Remove(old)
			c.stats.Evictions++
		}
		body := append([]byte(nil), o.Body...)
		c.entries[key] = c.order.PushFront(cacheEntry{key, body})
		c.stats.Bytes += len(body)
	}
	return o.Body, nil
}
