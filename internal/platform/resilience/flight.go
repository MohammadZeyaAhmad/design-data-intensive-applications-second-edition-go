package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

type flight[T any] struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	done                  chan struct{}
	waiters               int
	finished, invalidated bool
	background            bool
	value                 T
	err                   error
}
type FlightStats struct {
	Active    int   `json:"active"`
	Keys      int   `json:"keys"`
	Shared    int64 `json:"shared"`
	Rejected  int64 `json:"rejected"`
	Abandoned int64 `json:"abandoned"`
}
type Group[T any] struct {
	mu                          sync.Mutex
	routes                      map[string]*flight[T]
	active, limit               int
	timeout                     time.Duration
	closed                      bool
	drained                     chan struct{}
	once                        sync.Once
	shared, rejected, abandoned int64
}

func NewGroup[T any](limit int, timeout time.Duration) (*Group[T], error) {
	if limit < 1 || limit > 1024 || timeout < time.Millisecond || timeout > 30*time.Second {
		return nil, errors.New("invalid coalescer limits")
	}
	return &Group[T]{routes: map[string]*flight[T]{}, limit: limit, timeout: timeout, drained: make(chan struct{})}, nil
}
func (g *Group[T]) Do(ctx context.Context, key string, load func(context.Context) (T, error)) (out T, shared bool, err error) {
	if e := ctx.Err(); e != nil {
		return out, false, e
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return out, false, ErrDraining
	}
	f := g.routes[key]
	if f == nil {
		if g.active >= g.limit {
			g.rejected++
			g.mu.Unlock()
			return out, false, ErrOverloaded
		}
		owned, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.timeout)
		f = &flight[T]{ctx: owned, cancel: cancel, done: make(chan struct{})}
		g.routes[key] = f
		g.active++
		go g.run(key, f, load)
	} else {
		shared = true
		g.shared++
	}
	f.waiters++
	g.mu.Unlock()
	select {
	case <-f.done:
	case <-ctx.Done():
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	f.waiters--
	if !f.finished && !f.background && f.waiters == 0 {
		g.abandoned++
		f.cancel()
		if g.routes[key] == f {
			delete(g.routes, key)
		}
	}
	if ctx.Err() != nil {
		return out, shared, ctx.Err()
	}
	return f.value, shared, f.err
}

// Start admits at most one refresh per key without spawning a waiter for every
// cache hit. The group owns the bounded refresh until completion or shutdown.
func (g *Group[T]) Start(ctx context.Context, key string, load func(context.Context) (T, error)) bool {
	if ctx.Err() != nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.routes[key] != nil {
		return false
	}
	if g.active >= g.limit {
		g.rejected++
		return false
	}
	owned, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.timeout)
	f := &flight[T]{ctx: owned, cancel: cancel, done: make(chan struct{}), background: true}
	g.routes[key] = f
	g.active++
	go g.run(key, f, load)
	return true
}
func (g *Group[T]) run(key string, f *flight[T], load func(context.Context) (T, error)) {
	var value T
	var loadErr error
	func() {
		defer func() {
			if recover() != nil {
				loadErr = errors.New("shared loader panic")
			}
		}()
		value, loadErr = load(f.ctx)
	}()
	g.mu.Lock()
	defer g.mu.Unlock()
	if f.invalidated {
		loadErr = ErrInvalidated
	} else if loadErr == nil && f.ctx.Err() != nil {
		loadErr = f.ctx.Err()
	}
	f.value, f.err, f.finished = value, loadErr, true
	f.cancel()
	if g.routes[key] == f {
		delete(g.routes, key)
	}
	g.active--
	close(f.done)
	g.checkDrained()
}
func (g *Group[T]) Invalidate(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if f := g.routes[key]; f != nil {
		f.invalidated = true
		f.cancel()
		delete(g.routes, key)
	}
}
func (g *Group[T]) checkDrained() {
	if g.closed && g.active == 0 {
		g.once.Do(func() { close(g.drained) })
	}
}
func (g *Group[T]) Close(ctx context.Context) error {
	g.mu.Lock()
	g.closed = true
	for key, f := range g.routes {
		f.cancel()
		delete(g.routes, key)
	}
	g.checkDrained()
	g.mu.Unlock()
	select {
	case <-g.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (g *Group[T]) Stats() FlightStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return FlightStats{Active: g.active, Keys: len(g.routes), Shared: g.shared, Rejected: g.rejected, Abandoned: g.abandoned}
}
