package graphql

import (
	"context"
	"sync"
)

type future[T any] struct {
	done  chan struct{}
	value *T
	err   error
}
type batch[T any] struct {
	mu       sync.Mutex
	items    map[string]*future[T]
	fetch    func(context.Context, []string) ([]T, error)
	identity func(T) (string, bool)
}

func newBatch[T any](fetch func(context.Context, []string) ([]T, error), identity func(T) (string, bool)) *batch[T] {
	return &batch[T]{items: map[string]*future[T]{}, fetch: fetch, identity: identity}
}

// load reserves each key once, including negative and failed results. Waiting
// readers share its future. A cache exists only for one authenticated request.
func (b *batch[T]) load(ctx context.Context, keys []string) (map[string]*T, error) {
	if ctx.Err() != nil {
		return nil, unavailable()
	}
	b.mu.Lock()
	missing := []string{}
	waiting := map[string]*future[T]{}
	unique := map[string]bool{}
	for _, key := range keys {
		if b.items[key] == nil && !unique[key] {
			unique[key] = true
			missing = append(missing, key)
		}
	}
	if len(b.items)+len(missing) > 2000 {
		b.mu.Unlock()
		return nil, costError()
	}
	for _, key := range missing {
		b.items[key] = &future[T]{done: make(chan struct{})}
	}
	for _, key := range keys {
		waiting[key] = b.items[key]
	}
	b.mu.Unlock()
	if len(missing) > 0 {
		rows, e := b.fetch(ctx, missing)
		values := map[string]*T{}
		wanted := map[string]bool{}
		for _, key := range missing {
			wanted[key] = true
		}
		if e == nil {
			for _, row := range rows {
				key, valid := b.identity(row)
				if !valid || !wanted[key] || values[key] != nil {
					e = forbidden()
					break
				}
				copy := row
				values[key] = &copy
			}
		}
		if e != nil {
			if _, public := e.(*publicError); !public {
				e = unavailable()
			}
		}
		b.mu.Lock()
		for _, key := range missing {
			f := b.items[key]
			f.value, f.err = values[key], e
			close(f.done)
		}
		b.mu.Unlock()
	}
	out := map[string]*T{}
	for key, f := range waiting {
		select {
		case <-ctx.Done():
			return nil, unavailable()
		case <-f.done:
			if f.err != nil {
				return nil, f.err
			}
			out[key] = f.value
		}
	}
	return out, nil
}
