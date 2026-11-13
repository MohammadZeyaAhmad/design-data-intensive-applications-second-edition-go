package cache

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type Listener struct {
	cancel             context.CancelFunc
	done, ready        chan struct{}
	once               sync.Once
	events, reconnects atomic.Int64
}

func StartListener(parent context.Context, dsn string, store *Store) *Listener {
	ctx, cancel := context.WithCancel(parent)
	l := &Listener{cancel: cancel, done: make(chan struct{}), ready: make(chan struct{})}
	go func() {
		defer close(l.done)
		for ctx.Err() == nil {
			connected := func() bool {
				startup, stop := context.WithTimeout(ctx, time.Second)
				defer stop()
				conn, e := pgx.Connect(startup, dsn)
				if e != nil {
					return false
				}
				defer func() {
					cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_ = conn.Close(cleanup)
				}()
				if _, e = conn.Exec(startup, "LISTEN atlas_catalog_cache_v1"); e != nil {
					return false
				}
				stop()
				l.once.Do(func() { close(l.ready) })
				for ctx.Err() == nil {
					notification, e := conn.WaitForNotification(ctx)
					if e != nil {
						return false
					}
					if len(notification.Payload) > 512 {
						continue
					}
					var event struct {
						Tenant  string `json:"tenant"`
						SKU     string `json:"sku"`
						Version int64  `json:"version"`
					}
					if json.Unmarshal([]byte(notification.Payload), &event) != nil {
						continue
					}
					if !tenantPattern.MatchString(event.Tenant) || !skuPattern.MatchString(event.SKU) || event.Version < 1 {
						continue
					}
					l.events.Add(1)
					store.Invalidate(ctx, event.Tenant, event.SKU, event.Version)
				}
				return true
			}()
			if connected || ctx.Err() != nil {
				return
			}
			l.reconnects.Add(1)
			timer := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return l
}
func (l *Listener) Ready() <-chan struct{} { return l.ready }
func (l *Listener) Counts() (int64, int64) { return l.events.Load(), l.reconnects.Load() }
func (l *Listener) Close(ctx context.Context) error {
	l.cancel()
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
