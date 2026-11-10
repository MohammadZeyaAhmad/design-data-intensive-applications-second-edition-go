package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	"github.com/jackc/pgx/v5"
)

func (s *Store) PublishOne(ctx context.Context, p api.Publisher) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	m := api.Message{Version: 1, SchemaVersion: 1}
	var sku, warehouse string
	err = tx.QueryRow(ctx, `SELECT o.event_id::text,o.tenant_id,o.sku,o.warehouse_id,o.aggregate_version,o.event_type,o.payload,o.metadata FROM inventory.outbox o WHERE o.broker_published_at IS NULL AND NOT EXISTS(SELECT 1 FROM inventory.outbox earlier WHERE earlier.tenant_id=o.tenant_id AND earlier.sku=o.sku AND earlier.warehouse_id=o.warehouse_id AND earlier.aggregate_version<o.aggregate_version AND earlier.broker_published_at IS NULL) ORDER BY o.created_at,o.event_id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&m.ID, &m.TenantID, &sku, &warehouse, &m.Sequence, &m.Type, &m.Data, &m.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	m.StreamID = api.InventoryStream(sku, warehouse)
	if err = p.Publish(ctx, api.InventoryTopic, m); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE inventory.outbox SET broker_published_at=clock_timestamp() WHERE event_id=$1::uuid`, m.ID); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) ApplyMessage(ctx context.Context, m api.Message) error {
	if !m.Valid(api.InventoryTopic) || m.SchemaVersion != 1 {
		return api.ErrInvalid
	}
	sku, warehouse, ok := strings.Cut(m.StreamID, "/")
	if !ok {
		return api.ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var source bool
	err = tx.QueryRow(ctx, `SELECT event_type=$6 AND payload=$7::jsonb AND metadata=$8::jsonb FROM inventory.outbox WHERE event_id=$1::uuid AND tenant_id=$2 AND sku=$3 AND warehouse_id=$4 AND aggregate_version=$5 FOR UPDATE`, m.ID, m.TenantID, sku, warehouse, m.Sequence, m.Type, []byte(m.Data), m.Metadata).Scan(&source)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !source {
		return api.ErrSource
	}
	if err != nil {
		return err
	}
	e := event{ID: m.ID, Tenant: m.TenantID, SKU: sku, Warehouse: warehouse, Version: m.Sequence, Type: m.Type, Payload: m.Data}
	if err = applyProjection(ctx, tx, e); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE inventory.outbox SET processed_at=coalesce(processed_at,clock_timestamp()),quarantined=false,last_error=NULL WHERE event_id=$1::uuid`, m.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) RunExpiry(ctx context.Context, onError func(error)) error {
	for ctx.Err() == nil {
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		did, err := s.ExpireOne(step)
		cancel()
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		if !did || err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return ctx.Err()
}
