package telemetry

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type suppressKey struct{}
type Audit struct {
	Runtime *Runtime
	Pool    *pgxpool.Pool
}

// Observe reads all signals in one database snapshot. A stale projection is lag;
// differing values at equal versions are a separate integrity failure.
func (a Audit) Observe(parent context.Context) error {
	a.Runtime.AuditEnabled.Set(1)
	ctx, cancel := context.WithTimeout(context.WithValue(parent, suppressKey{}, true), time.Second)
	defer cancel()
	var pending, integrity int64
	var age float64
	err := a.Pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM inventory.outbox WHERE processed_at IS NULL),
 (SELECT coalesce(greatest(0,extract(epoch FROM clock_timestamp()-min(created_at))),0)::double precision FROM inventory.outbox WHERE processed_at IS NULL),
 (SELECT count(*) FROM inventory.stock s JOIN inventory.stock_views v USING(tenant_id,sku,warehouse_id)
  WHERE s.version=v.version AND (s.on_hand,s.reserved,s.allocated) IS DISTINCT FROM (v.on_hand,v.reserved,v.allocated))`).Scan(&pending, &age, &integrity)
	if err != nil {
		a.Runtime.AuditUp.Set(0)
		return err
	}
	a.Runtime.Pending.Set(float64(pending))
	a.Runtime.Lag.Set(age)
	a.Runtime.Integrity.Set(float64(integrity))
	a.Runtime.AuditAt.Set(float64(time.Now().Unix()))
	a.Runtime.AuditUp.Set(1)
	return nil
}
func (r *Runtime) StartAudit(parent context.Context, pool *pgxpool.Pool) {
	if !r.Options.Enabled {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	r.mu.Lock()
	r.closers = append(r.closers, func(shutdown context.Context) error {
		cancel()
		select {
		case <-done:
			return nil
		case <-shutdown.Done():
			return shutdown.Err()
		}
	})
	r.mu.Unlock()
	go func() {
		defer close(done)
		audit := Audit{Runtime: r, Pool: pool}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			_ = audit.Observe(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
