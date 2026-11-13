// Package resilience bounds resource use and retry amplification at actual I/O
// seams. It does not change a business command's identity or transaction scope.
package resilience

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrOverloaded  = errors.New("resource admission is full")
	ErrDraining    = errors.New("resource is draining")
	ErrCircuitOpen = errors.New("dependency circuit is open")
	ErrInvalidated = errors.New("shared load was invalidated")
)

type BulkheadStats struct {
	Active    int   `json:"active"`
	Queued    int   `json:"queued"`
	MaxActive int   `json:"max_active"`
	MaxQueued int   `json:"max_queued"`
	Rejected  int64 `json:"rejected"`
	Canceled  int64 `json:"canceled"`
	Draining  bool  `json:"draining"`
}
type waiter struct {
	ready   chan struct{}
	granted bool
	err     error
}
type Bulkhead struct {
	mu                           sync.Mutex
	limit, queueLimit            int
	active, maxActive, maxQueued int
	queue                        []*waiter
	closed                       bool
	drained                      chan struct{}
	drainOnce                    sync.Once
	rejected, canceled           int64
}
type Permit struct {
	once    sync.Once
	release func()
}

func (p *Permit) Release() {
	if p != nil {
		p.once.Do(p.release)
	}
}
func NewBulkhead(active, queued int) (*Bulkhead, error) {
	if active < 1 || active > 1024 || queued < 0 || queued > 4096 {
		return nil, errors.New("invalid bulkhead limits")
	}
	return &Bulkhead{limit: active, queueLimit: queued, drained: make(chan struct{})}, nil
}
func (b *Bulkhead) permit() *Permit {
	return &Permit{release: func() { b.mu.Lock(); defer b.mu.Unlock(); b.active--; b.dispatch() }}
}
func (b *Bulkhead) Acquire(ctx context.Context) (*Permit, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrDraining
	}
	if b.active < b.limit && len(b.queue) == 0 {
		b.active++
		if b.active > b.maxActive {
			b.maxActive = b.active
		}
		b.mu.Unlock()
		return b.permit(), nil
	}
	if len(b.queue) >= b.queueLimit {
		b.rejected++
		b.mu.Unlock()
		return nil, ErrOverloaded
	}
	w := &waiter{ready: make(chan struct{})}
	b.queue = append(b.queue, w)
	if len(b.queue) > b.maxQueued {
		b.maxQueued = len(b.queue)
	}
	b.mu.Unlock()
	select {
	case <-w.ready:
	case <-ctx.Done():
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := ctx.Err(); e != nil {
		b.canceled++
		if w.granted {
			b.active--
			b.dispatch()
		} else {
			for i, v := range b.queue {
				if v == w {
					b.queue = append(b.queue[:i], b.queue[i+1:]...)
					break
				}
			}
		}
		return nil, e
	}
	if w.err != nil {
		return nil, w.err
	}
	if !w.granted {
		return nil, ErrDraining
	}
	return b.permit(), nil
}
func (b *Bulkhead) dispatch() {
	for !b.closed && b.active < b.limit && len(b.queue) > 0 {
		w := b.queue[0]
		b.queue = b.queue[1:]
		w.granted = true
		b.active++
		if b.active > b.maxActive {
			b.maxActive = b.active
		}
		close(w.ready)
	}
	if b.closed && b.active == 0 {
		b.drainOnce.Do(func() { close(b.drained) })
	}
}
func (b *Bulkhead) BeginDrain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, w := range b.queue {
		w.err = ErrDraining
		close(w.ready)
	}
	b.queue = nil
	b.dispatch()
}
func (b *Bulkhead) Drain(ctx context.Context) error {
	b.BeginDrain()
	select {
	case <-b.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *Bulkhead) Stats() BulkheadStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BulkheadStats{Active: b.active, Queued: len(b.queue), MaxActive: b.maxActive, MaxQueued: b.maxQueued, Rejected: b.rejected, Canceled: b.canceled, Draining: b.closed}
}
