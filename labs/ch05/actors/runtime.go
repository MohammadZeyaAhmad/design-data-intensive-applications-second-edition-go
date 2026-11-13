package actors

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrBackpressure = errors.New("actor mailbox is full")
	ErrCapacity     = errors.New("actor registry is full")
	ErrStale        = errors.New("stale actor reference")
	ErrStopped      = errors.New("actor is stopped")
	ErrCrashed      = errors.New("actor crashed")
	ErrRestarted    = errors.New("message lost during actor restart")
)

type Ref struct {
	Key         Key    `json:"key"`
	Incarnation string `json:"incarnation"`
	Generation  uint64 `json:"generation"`
}
type Acceptance struct {
	ID  string `json:"id"`
	Ref Ref    `json:"ref"`
}
type DeadLetter struct {
	ID     string `json:"id"`
	Ref    Ref    `json:"ref"`
	Reason string `json:"reason"`
}
type Stats struct {
	Accepted         int64 `json:"accepted"`
	Processed        int64 `json:"processed"`
	Backpressure     int64 `json:"backpressure"`
	Restarts         int64 `json:"restarts"`
	Dropped          int64 `json:"dropped"`
	AbandonedReplies int64 `json:"abandoned_replies"`
}
type Hooks struct {
	Before func(context.Context, Message) error
	After  func(context.Context, Message, Reply) error
}
type Options struct {
	Mailbox          int
	MaxActors        int
	RetainedReceipts int
	DeadLetters      int
	MaxRestarts      int
	RestartWindow    time.Duration
	Reader           int
	Factory          Factory
	Hooks            Hooks
}

func DefaultOptions() Options {
	return Options{Mailbox: 16, MaxActors: 32, RetainedReceipts: 128, DeadLetters: 32, MaxRestarts: 3, RestartWindow: time.Minute, Reader: 2}
}

type Runtime struct {
	mu                                                              sync.Mutex
	cells                                                           map[Key]*cell
	closed                                                          bool
	incarnation                                                     string
	options                                                         Options
	ctx                                                             context.Context
	cancel                                                          context.CancelFunc
	letterMu                                                        sync.Mutex
	letters                                                         []DeadLetter
	accepted, processed, backpressure, restarts, dropped, abandoned atomic.Int64
}
type delivery struct {
	ref      Ref
	message  Message
	reply    chan result
	waiter   context.Context
	position Position
}
type result struct {
	reply Reply
	err   error
}
type cell struct {
	mu      sync.Mutex
	ref     Ref
	stopped bool
	queue   chan delivery
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	runtime *Runtime
}

func New(parent context.Context, o Options) (*Runtime, error) {
	if o.Mailbox < 1 || o.Mailbox > 128 || o.MaxActors < 1 || o.MaxActors > 64 || o.RetainedReceipts < 1 || o.RetainedReceipts > 128 || o.DeadLetters < 1 || o.DeadLetters > 128 || o.MaxRestarts < 0 || o.MaxRestarts > 8 || o.RestartWindow < time.Second || o.RestartWindow > time.Hour || o.Reader < 1 || o.Reader > 2 {
		return nil, ErrInvalid
	}
	var boot [16]byte
	if _, e := rand.Read(boot[:]); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithCancel(parent)
	if o.Factory == nil {
		o.Factory = MemoryFactory(o.RetainedReceipts)
	}
	return &Runtime{cells: map[Key]*cell{}, incarnation: hex.EncodeToString(boot[:]), options: o, ctx: ctx, cancel: cancel}, nil
}
func (r *Runtime) Resolve(key Key) (Ref, error) {
	key = key.Normalize()
	if !key.Valid() {
		return Ref{}, ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return Ref{}, ErrStopped
	}
	c, ok := r.cells[key]
	if !ok {
		if len(r.cells) == r.options.MaxActors {
			return Ref{}, ErrCapacity
		}
		ctx, cancel := context.WithCancel(r.ctx)
		c = &cell{ref: Ref{Key: key, Incarnation: r.incarnation, Generation: 1}, queue: make(chan delivery, r.options.Mailbox), ctx: ctx, cancel: cancel, done: make(chan struct{}), runtime: r}
		r.cells[key] = c
		go c.run()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return Ref{}, ErrStopped
	}
	return c.ref, nil
}
func (r *Runtime) Tell(ctx context.Context, ref Ref, raw []byte) (Acceptance, error) {
	d, e := r.prepare(ctx, ref, raw)
	if e != nil {
		return Acceptance{}, e
	}
	if e = r.admit(d); e != nil {
		return Acceptance{}, e
	}
	return Acceptance{ID: d.message.ID, Ref: ref}, nil
}
func (r *Runtime) Ask(ctx context.Context, ref Ref, raw []byte) (Reply, error) {
	d, e := r.prepare(ctx, ref, raw)
	if e != nil {
		return Reply{}, e
	}
	d.reply = make(chan result, 1)
	d.waiter = ctx
	if e = r.admit(d); e != nil {
		return Reply{}, e
	}
	select {
	case out := <-d.reply:
		return out.reply, out.err
	case <-ctx.Done():
		return Reply{}, ctx.Err()
	}
}
func (r *Runtime) prepare(ctx context.Context, ref Ref, raw []byte) (delivery, error) {
	if e := ctx.Err(); e != nil {
		return delivery{}, e
	}
	m, e := Decode(raw, r.options.Reader)
	if e != nil {
		return delivery{}, e
	}
	if ref.Key != m.Key || ref.Incarnation != r.incarnation {
		return delivery{}, ErrStale
	}
	return delivery{ref: ref, message: m, position: PositionFrom(ctx)}, nil
}
func (r *Runtime) admit(d delivery) error {
	r.mu.Lock()
	c := r.cells[d.ref.Key]
	closed := r.closed || r.ctx.Err() != nil
	r.mu.Unlock()
	if closed || c == nil {
		return ErrStopped
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || c.ctx.Err() != nil {
		return ErrStopped
	}
	if c.ref != d.ref {
		return ErrStale
	}
	select {
	case c.queue <- d:
		r.accepted.Add(1)
		return nil
	default:
		r.backpressure.Add(1)
		return ErrBackpressure
	}
}
func (c *cell) run() {
	defer close(c.done)
	defer c.cancel()
	processor := c.runtime.options.Factory(c.ref.Key)
	restartCount := 0
	window := time.Now()
	for {
		select {
		case <-c.ctx.Done():
			c.mu.Lock()
			c.stopped = true
			c.drain(ErrStopped)
			c.mu.Unlock()
			return
		case d := <-c.queue:
			c.mu.Lock()
			valid := d.ref == c.ref && !c.stopped && c.ctx.Err() == nil
			c.mu.Unlock()
			if !valid {
				c.fail(d, ErrRestarted)
				continue
			}
			out, e, crashed := c.invoke(processor, d)
			if crashed {
				c.runtime.dropped.Add(1)
				if d.reply != nil {
					c.runtime.letter(d, ErrCrashed)
				}
				c.mu.Lock()
				c.ref.Generation++
				c.drain(ErrRestarted)
				if time.Since(window) > c.runtime.options.RestartWindow {
					window = time.Now()
					restartCount = 0
				}
				restartCount++
				if restartCount > c.runtime.options.MaxRestarts {
					c.stopped = true
					c.mu.Unlock()
					c.finish(d, Reply{}, ErrCrashed)
					return
				}
				c.runtime.restarts.Add(1)
				c.mu.Unlock()
				processor = c.runtime.options.Factory(d.ref.Key)
				// Publish the failed Ask result only after its old generation
				// is fenced and the replacement processor is ready.
				c.finish(d, Reply{}, ErrCrashed)
				continue
			}
			c.runtime.processed.Add(1)
			out.Ref = d.ref
			c.finish(d, out, e)
		}
	}
}
func (c *cell) invoke(process Processor, d delivery) (out Reply, err error, crashed bool) {
	m := d.message
	processing := WithPosition(c.ctx, d.position)
	defer func() {
		if recover() != nil {
			out = Reply{}
			err = ErrCrashed
			crashed = true
		}
	}()
	if hook := c.runtime.options.Hooks.Before; hook != nil {
		if err = hook(processing, m); err != nil {
			return out, err, false
		}
	}
	out, err = process(processing, m)
	if err == nil {
		if hook := c.runtime.options.Hooks.After; hook != nil {
			err = hook(processing, m, out)
		}
	}
	return out, err, false
}
func (c *cell) finish(d delivery, out Reply, err error) {
	if d.reply != nil {
		if d.waiter.Err() != nil {
			c.runtime.abandoned.Add(1)
		}
		d.reply <- result{reply: out, err: err}
	} else if err != nil {
		c.runtime.letter(d, err)
	}
}
func (c *cell) fail(d delivery, e error) {
	c.runtime.dropped.Add(1)
	c.runtime.letter(d, e)
	if d.reply != nil {
		c.finish(d, Reply{}, e)
	}
}
func (c *cell) drain(e error) {
	for {
		select {
		case d := <-c.queue:
			c.fail(d, e)
		default:
			return
		}
	}
}
func (r *Runtime) letter(d delivery, e error) {
	r.letterMu.Lock()
	defer r.letterMu.Unlock()
	if len(r.letters) == r.options.DeadLetters {
		copy(r.letters, r.letters[1:])
		r.letters = r.letters[:len(r.letters)-1]
	}
	r.letters = append(r.letters, DeadLetter{ID: d.message.ID, Ref: d.ref, Reason: e.Error()})
}
func (r *Runtime) DeadLetters() []DeadLetter {
	r.letterMu.Lock()
	defer r.letterMu.Unlock()
	return append([]DeadLetter(nil), r.letters...)
}
func (r *Runtime) Stats() Stats {
	return Stats{Accepted: r.accepted.Load(), Processed: r.processed.Load(), Backpressure: r.backpressure.Load(), Restarts: r.restarts.Load(), Dropped: r.dropped.Load(), AbandonedReplies: r.abandoned.Load()}
}
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	cells := make([]*cell, 0, len(r.cells))
	for _, c := range r.cells {
		cells = append(cells, c)
	}
	r.mu.Unlock()
	for _, c := range cells {
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
