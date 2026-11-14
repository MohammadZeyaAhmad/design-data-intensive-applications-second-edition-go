package objectstorage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Memory supplies deterministic, linearizable contract/fault schedules. It
// models acknowledged objects as durable; it is not a durable backend itself.
type Memory struct {
	mu       sync.Mutex
	objects  map[string]Object
	revision uint64
}

func NewMemory() *Memory { return &Memory{objects: map[string]Object{}} }
func (s *Memory) Get(ctx context.Context, key string) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if !validKey(key) {
		return Object{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	o.Body = append([]byte(nil), o.Body...)
	return o, nil
}
func (s *Memory) Put(ctx context.Context, key string, body []byte, c Condition) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !validKey(key) || !c.valid() || len(body) > MaxObjectBytes {
		return "", ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prior, ok := s.objects[key]
	if (c.Absent && ok) || (!c.Absent && (!ok || prior.Version != c.Version)) {
		return "", ErrConflict
	}
	if !ok && len(s.objects) >= MaxObjects {
		return "", ErrLimit
	}
	s.revision++
	version := fmt.Sprintf(`"memory-%d"`, s.revision)
	s.objects[key] = Object{append([]byte(nil), body...), version}
	return version, nil
}
func (s *Memory) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := []string{}
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
func (s *Memory) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validKey(key) {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}
