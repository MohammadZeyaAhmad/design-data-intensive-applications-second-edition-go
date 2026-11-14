package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/changereservation"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry"
	"github.com/jackc/pgx/v5"
)

type event struct {
	ID, Tenant, SKU, Type, Warehouse string
	Version                          int64
	Payload                          []byte
	Metadata                         envelope.Metadata
}

// ProcessOne is a local transactional dispatcher, not a Kafka publisher. The
// earlier-event predicate prevents SKIP LOCKED from overtaking a locked event
// from the same aggregate; unrelated streams remain independently processable.
func (s *Store) ProcessOne(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var e event
	err = tx.QueryRow(ctx, `SELECT o.event_id::text,o.tenant_id,o.sku,o.aggregate_version,o.event_type,o.payload,o.warehouse_id,o.metadata
        FROM inventory.outbox o
        WHERE o.processed_at IS NULL AND NOT o.quarantined AND o.available_at <= now()
          AND NOT EXISTS (SELECT 1 FROM inventory.outbox earlier WHERE earlier.tenant_id=o.tenant_id AND earlier.sku=o.sku AND earlier.warehouse_id=o.warehouse_id AND earlier.aggregate_version<o.aggregate_version AND earlier.processed_at IS NULL)
        ORDER BY o.created_at,o.event_id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&e.ID, &e.Tenant, &e.SKU, &e.Version, &e.Type, &e.Payload, &e.Warehouse, &e.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ctx, finish := telemetry.Consumer(ctx, e.Metadata.TraceParent, "inventory.projection.apply", "other")
	defer func() { finish(err) }()
	if err = applyProjection(ctx, tx, e); err != nil {
		rollback(tx)
		if ctx.Err() == nil {
			// A quarantined event still blocks later events of its own stream.
			_, updateErr := s.Pool.Exec(ctx, `UPDATE inventory.outbox SET attempts=attempts+1, available_at=now()+interval '1 second', quarantined=(attempts+1>=5), last_error='projection_failed' WHERE event_id=$1::uuid AND processed_at IS NULL`, e.ID)
			if updateErr != nil {
				return true, errors.Join(err, updateErr)
			}
		}
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE inventory.outbox SET processed_at=now(),last_error=NULL WHERE event_id=$1::uuid`, e.ID); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}

func applyProjection(ctx context.Context, tx pgx.Tx, e event) error {
	var out ports.Outcome
	if err := json.Unmarshal(e.Payload, &out); err != nil {
		return fmt.Errorf("invalid event payload")
	}
	state := out.Stock
	if state.TenantID != e.Tenant || state.SKU != e.SKU || state.Version != e.Version || dbWarehouse(state.WarehouseID) != e.Warehouse {
		return domain.ErrCorruptState
	}
	if _, err := domain.RestoreStock(state); err != nil {
		return err
	}
	switch e.Type {
	case "inventory.stock-received.v1", "inventory.stock-reserved.v1", "inventory.reservation-committed.v1", "inventory.reservation-released.v1", "inventory.reservation-expired.v1", "inventory.allocation-reversed.v1", "inventory.allocation-shipped.v1":
	default:
		return fmt.Errorf("unsupported inventory event type")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO inventory.projection_receipts (projection_name,tenant_id,event_id) VALUES ('stock-v1',$1,$2::uuid) ON CONFLICT DO NOTHING`, e.Tenant, e.ID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	var version int64
	err = tx.QueryRow(ctx, `SELECT version FROM inventory.stock_views WHERE tenant_id=$1 AND sku=$2 AND warehouse_id=$3 FOR UPDATE`, e.Tenant, e.SKU, e.Warehouse).Scan(&version)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if state.Version != version+1 {
		return fmt.Errorf("projection version gap: have %d, received %d", version, state.Version)
	}
	_, err = tx.Exec(ctx, `INSERT INTO inventory.stock_views (tenant_id,sku,on_hand,reserved,allocated,version,warehouse_id) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,sku,warehouse_id) DO UPDATE SET on_hand=EXCLUDED.on_hand,reserved=EXCLUDED.reserved,allocated=EXCLUDED.allocated,version=EXCLUDED.version`, state.TenantID, state.SKU, state.OnHand, state.Reserved, state.Allocated, state.Version, e.Warehouse)
	return err
}

func (s *Store) Run(ctx context.Context, onError func(error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		expired, expiryErr := s.ExpireOne(step)
		if expiryErr != nil && ctx.Err() == nil {
			onError(expiryErr)
		}
		didWork, err := s.ProcessOne(step)
		cancel()
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		if (!didWork && !expired) || err != nil || expiryErr != nil {
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

// ExpireOne claims a due reservation and persists release plus event atomically.
// A crash before commit releases its locks; after commit, the row is no longer active.
func (s *Store) ExpireOne(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var tenant, id string
	var metadata envelope.Metadata
	err = tx.QueryRow(ctx, `SELECT tenant_id,id::text,metadata FROM inventory.reservations WHERE status='active' AND expires_at<=clock_timestamp() ORDER BY expires_at,tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&tenant, &id, &metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ctx, err = envelope.Worker(ctx, metadata, "inventory-expiry", id)
	if err != nil {
		return true, err
	}
	_, err = changereservation.Apply(ctx, &transaction{tx: tx, tenant: tenant}, changereservation.Command{ID: id, Target: domain.ReservationExpired})
	if errors.Is(err, domain.ErrReservationNotDue) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
