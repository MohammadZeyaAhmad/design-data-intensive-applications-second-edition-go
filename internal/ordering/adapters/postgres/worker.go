package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/jackc/pgx/v5"
)

// ProcessOne uses the same stream-local projection lock as Rebuild. Locks are
// always acquired before outbox/view locks, avoiding a rebuild/consumer deadlock.
func (s *Store) ProcessOne(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT o.tenant_id,o.order_id::text FROM ordering.outbox o WHERE o.processed_at IS NULL AND NOT o.quarantined AND o.available_at<=now() AND NOT EXISTS(SELECT 1 FROM ordering.outbox earlier WHERE earlier.tenant_id=o.tenant_id AND earlier.order_id=o.order_id AND earlier.version<o.version AND earlier.processed_at IS NULL) ORDER BY o.available_at,o.tenant_id,o.order_id LIMIT 32`)
	if err != nil {
		return false, err
	}
	type stream struct{ tenant, id string }
	var candidates []stream
	for rows.Next() {
		var c stream
		if err = rows.Scan(&c.tenant, &c.id); err != nil {
			rows.Close()
			return false, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, c := range candidates {
		acquired, err := projectionLock(ctx, tx, c.tenant, c.id, false)
		if err != nil {
			return false, err
		}
		if !acquired {
			continue
		}
		var event domain.Event
		var eventID string
		err = tx.QueryRow(ctx, `SELECT e.event_id::text,e.version,e.event_type,e.schema_version,e.payload FROM ordering.outbox o JOIN ordering.events e ON e.tenant_id=o.tenant_id AND e.order_id=o.order_id AND e.version=o.version AND e.event_id=o.event_id WHERE o.tenant_id=$1 AND o.order_id=$2::uuid AND o.processed_at IS NULL AND NOT o.quarantined AND o.available_at<=now() AND NOT EXISTS(SELECT 1 FROM ordering.outbox earlier WHERE earlier.tenant_id=o.tenant_id AND earlier.order_id=o.order_id AND earlier.version<o.version AND earlier.processed_at IS NULL) ORDER BY o.version FOR UPDATE OF o SKIP LOCKED LIMIT 1`, c.tenant, c.id).Scan(&eventID, &event.Version, &event.Type, &event.SchemaVersion, &event.Data)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, err
		}
		if err = applyEvent(ctx, tx, c.tenant, c.id, eventID, event); err != nil {
			rollback(tx)
			if ctx.Err() == nil {
				_, retryErr := s.Pool.Exec(ctx, `UPDATE ordering.outbox SET attempts=attempts+1,available_at=now()+interval '1 second',quarantined=(attempts+1>=5) WHERE tenant_id=$1 AND order_id=$2::uuid AND version=$3 AND processed_at IS NULL`, c.tenant, c.id, event.Version)
				if retryErr != nil {
					return true, errors.Join(err, retryErr)
				}
			}
			return true, err
		}
		if _, err = tx.Exec(ctx, `UPDATE ordering.outbox SET processed_at=now() WHERE tenant_id=$1 AND order_id=$2::uuid AND version=$3`, c.tenant, c.id, event.Version); err != nil {
			return true, err
		}
		return true, tx.Commit(ctx)
	}
	return false, nil
}

func applyEvent(ctx context.Context, tx pgx.Tx, tenant, id, eventID string, event domain.Event) error {
	tag, err := tx.Exec(ctx, `INSERT INTO ordering.projection_receipts (tenant_id,event_id) VALUES ($1,$2::uuid) ON CONFLICT DO NOTHING`, tenant, eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	var body []byte
	var version int64
	err = tx.QueryRow(ctx, `SELECT version,body FROM ordering.order_views WHERE tenant_id=$1 AND order_id=$2::uuid FOR UPDATE`, tenant, id).Scan(&version, &body)
	var order *domain.Order
	if errors.Is(err, pgx.ErrNoRows) {
		order, err = domain.New(tenant, id)
	} else if err == nil {
		var state domain.State
		if jsonErr := json.Unmarshal(body, &state); jsonErr != nil {
			return jsonErr
		}
		if state.Version != version || state.TenantID != tenant || state.ID != id {
			return domain.ErrInvalidEvent
		}
		order, err = domain.Restore(state)
	}
	if err != nil {
		return err
	}
	if err = order.Apply(event); err != nil {
		return err
	}
	return saveView(ctx, tx, order.Snapshot())
}

func (s *Store) Run(ctx context.Context, onError func(error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		did, err := s.ProcessOne(step)
		cancel()
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		if !did || err != nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
