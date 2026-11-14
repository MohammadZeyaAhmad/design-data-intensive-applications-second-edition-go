package readrouting

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Replica struct {
	Label string
	Pool  *pgxpool.Pool
}
type Options struct {
	Primary  *pgxpool.Pool
	Replicas []Replica
	Key      []byte
	Shard    string
}
type ReadOptions struct {
	Token, Mode, OnLag string
	Wait               time.Duration
}
type Meta struct{ Token, Source, Reason string }
type Router struct {
	primary   *pgxpool.Pool
	replicas  []Replica
	signer    *Signer
	authority Point
	next      atomic.Uint64
	probes    map[string]*probeState
	pending   atomic.Uint64
	fallbacks atomic.Uint64
}

func New(ctx context.Context, o Options) (*Router, error) {
	if o.Primary == nil || !label(o.Shard) || len(o.Replicas) > 8 {
		return nil, ErrContract
	}
	seen := map[string]bool{"primary": true}
	for _, r := range o.Replicas {
		if !label(r.Label) || seen[r.Label] || r.Pool == nil {
			return nil, ErrContract
		}
		seen[r.Label] = true
	}
	s, e := NewSigner(o.Key, 5*time.Minute)
	if e != nil {
		return nil, e
	}
	r := &Router{primary: o.Primary, replicas: append([]Replica(nil), o.Replicas...), signer: s, authority: Point{Shard: o.Shard}}
	r.probes = make(map[string]*probeState, len(o.Replicas))
	for _, n := range o.Replicas {
		r.probes[n.Label] = &probeState{}
	}
	p, recovery, e := probe(ctx, o.Primary, false)
	if e != nil {
		return nil, ErrUnavailable
	}
	if recovery {
		return nil, ErrHistory
	}
	r.authority.Source = p.Source
	r.authority.Epoch = p.Epoch
	return r, nil
}

type rowQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func probe(ctx context.Context, q rowQuery, replica bool) (Point, bool, error) {
	// A primary insert position includes commits already visible even when
	// synchronous_commit=off. A flush position need not bound those commits.
	sql := `SELECT i.source_id::text,i.epoch,pg_is_in_recovery(),pg_current_wal_insert_lsn()::text FROM platform.read_identity() i`
	if replica {
		sql = `SELECT i.source_id::text,i.epoch,pg_is_in_recovery(),COALESCE(pg_last_wal_replay_lsn(),'0/0'::pg_lsn)::text FROM platform.read_identity() i`
	}
	var p Point
	var recovery bool
	var lsn string
	err := q.QueryRow(ctx, sql).Scan(&p.Source, &p.Epoch, &recovery, &lsn)
	if err != nil {
		return p, recovery, err
	}
	p.Position, err = ParseLSN(lsn)
	return p, recovery, err
}
func (r *Router) same(p Point) bool {
	return p.Source == r.authority.Source && p.Epoch == r.authority.Epoch
}
func (r *Router) decode(tenant, token string) (Vector, Point, error) {
	v, e := r.signer.Decode(tenant, token)
	if e != nil {
		return v, Point{}, e
	}
	p := r.authority
	for _, x := range v.Points {
		if x.Shard == p.Shard {
			if !r.same(x) {
				return v, x, ErrHistory
			}
			return v, x, nil
		}
	}
	return v, p, nil
}
func (r *Router) capture(ctx context.Context, required Point) (Point, error) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	p, recovery, e := probe(ctx, r.primary, false)
	if e != nil {
		return p, ErrUnavailable
	}
	if recovery || !r.same(p) || p.Position < required.Position {
		return p, ErrHistory
	}
	p.Shard = r.authority.Shard
	return p, nil
}

// Verify runs before a command. It never silently drops a token from another
// authority or history, even when primary fallback was requested.
func (r *Router) Verify(ctx context.Context, tenant, token string) error {
	_, p, e := r.decode(tenant, token)
	if e != nil {
		return e
	}
	_, e = r.capture(ctx, p)
	return e
}

// AfterWrite must be called after commit. Its failure does not undo that commit.
func (r *Router) AfterWrite(ctx context.Context, tenant, token string) (Meta, error) {
	v, p, e := r.decode(tenant, token)
	if e != nil {
		return Meta{}, e
	}
	p, e = r.capture(ctx, p)
	if e != nil {
		return Meta{}, e
	}
	t, e := r.signer.Advance(tenant, v, p)
	return Meta{Token: t, Source: "primary", Reason: "committed-write"}, e
}
func (r *Router) readAt(ctx context.Context, pool *pgxpool.Pool, replica bool, fn func(context.Context, pgx.Tx) error) error {
	tx, e := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return fmt.Errorf("begin snapshot: %w", ErrUnavailable)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	// First statement establishes the RR snapshot. Gate replay BEFORE BeginTx.
	p, recovery, e := probe(ctx, tx, replica)
	if e != nil {
		return fmt.Errorf("snapshot identity: %w", ErrUnavailable)
	}
	if recovery != replica || !r.same(p) {
		return ErrHistory
	}
	if e = fn(ctx, tx); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return fmt.Errorf("finish snapshot: %w", ErrUnavailable)
	}
	return nil
}
func (r *Router) Read(ctx context.Context, tenant string, o ReadOptions, fn func(context.Context, pgx.Tx) error) (Meta, error) {
	if o.Mode != "primary" && o.Mode != "replica" || o.OnLag != "pending" && o.OnLag != "primary" || o.Wait < 0 || o.Wait > 2*time.Second {
		return Meta{}, ErrContract
	}
	v, required, e := r.decode(tenant, o.Token)
	if e != nil {
		return Meta{}, e
	}
	pool := r.primary
	source := "primary"
	reason := "requested-primary"
	if o.Mode == "replica" {
		pool = nil
		reason = "replica-ready"
		order := make([]Replica, 0, len(r.replicas))
		start := 0
		if len(r.replicas) > 0 {
			start = int((r.next.Add(1) - 1) % uint64(len(r.replicas)))
		}
		for _, n := range r.replicas {
			if n.Label == required.Sticky {
				order = append(order, n)
			}
		}
		for i := range r.replicas {
			n := r.replicas[(start+i)%len(r.replicas)]
			if n.Label != required.Sticky {
				order = append(order, n)
			}
		}
		deadline := time.Now().Add(o.Wait)
		for {
			for _, n := range order {
				// One immediate probe is allowed with wait=0. All pool/probe
				// operations are bounded, including unavailable endpoints.
				budget := 100 * time.Millisecond
				if o.Wait > 0 {
					left := time.Until(deadline)
					if left <= 0 {
						break
					}
					if left < budget {
						budget = left
					}
				}
				pc, cancel := context.WithTimeout(ctx, budget)
				p, recovery, err := probe(pc, n.Pool, true)
				cancel()
				r.recordProbe(n.Label, err == nil && recovery && r.same(p), required.Position, p.Position)
				if err == nil && recovery && r.same(p) && p.Position >= required.Position {
					pool = n.Pool
					source = n.Label
					break
				}
				if ctx.Err() != nil {
					return Meta{}, ctx.Err()
				}
			}
			if pool != nil || o.Wait == 0 || time.Now().After(deadline) {
				break
			}
			d := time.Until(deadline)
			if d > 10*time.Millisecond {
				d = 10 * time.Millisecond
			}
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return Meta{}, ctx.Err()
			case <-timer.C:
			}
		}
		if pool == nil {
			if o.OnLag == "pending" {
				r.pending.Add(1)
				return Meta{}, ErrPending
			}
			pool = r.primary
			r.fallbacks.Add(1)
			source = "primary"
			reason = "replica-timeout-primary"
		}
	}
	if source == "primary" {
		if _, e = r.capture(ctx, required); e != nil {
			return Meta{}, e
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if e = r.readAt(readCtx, pool, source != "primary", fn); e != nil {
		return Meta{}, e
	}
	// This conservative primary fence bounds everything the replica could
	// have returned. It may include MORE commits than this read's snapshot.
	p, e := r.capture(ctx, required)
	if e != nil {
		return Meta{}, e
	}
	if source != "primary" {
		p.Sticky = source
	}
	t, e := r.signer.Advance(tenant, v, p)
	if e != nil {
		return Meta{}, fmt.Errorf("advance session: %w", e)
	}
	return Meta{Token: t, Source: source, Reason: reason}, nil
}
