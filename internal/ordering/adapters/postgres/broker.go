package postgres

import (
	"context"
	"encoding/json"
	"errors"

	fulfill "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) PublishOne(ctx context.Context, p api.Publisher) (bool, error) {
	return s.publishEvent(ctx, p, "", "")
}
func (s *Store) PublishStream(ctx context.Context, p api.Publisher, tenant, id string) (bool, error) {
	if !domain.ValidTenant(tenant) || !domain.ValidID(id) {
		return false, api.ErrInvalid
	}
	return s.publishEvent(ctx, p, tenant, id)
}
func (s *Store) publishEvent(ctx context.Context, p api.Publisher, tenant, id string) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	m := api.Message{Version: 1}
	err = tx.QueryRow(ctx, `SELECT e.event_id::text,e.tenant_id,e.order_id::text,e.version,e.event_type,e.schema_version,e.payload,e.metadata FROM ordering.outbox o JOIN ordering.events e ON e.tenant_id=o.tenant_id AND e.order_id=o.order_id AND e.version=o.version AND e.event_id=o.event_id WHERE o.broker_published_at IS NULL AND ($1='' OR o.tenant_id=$1) AND ($2='' OR o.order_id::text=$2) AND NOT EXISTS(SELECT 1 FROM ordering.outbox earlier WHERE earlier.tenant_id=o.tenant_id AND earlier.order_id=o.order_id AND earlier.version<o.version AND earlier.broker_published_at IS NULL) ORDER BY o.tenant_id,o.order_id,o.version FOR UPDATE OF o SKIP LOCKED LIMIT 1`, tenant, id).Scan(&m.ID, &m.TenantID, &m.StreamID, &m.Sequence, &m.Type, &m.SchemaVersion, &m.Data, &m.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = p.Publish(ctx, api.OrderingTopic, m); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.outbox SET broker_published_at=clock_timestamp() WHERE event_id=$1::uuid`, m.ID); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) ApplyMessage(ctx context.Context, m api.Message) error {
	if !m.Valid(api.OrderingTopic) || !domain.ValidID(m.StreamID) {
		return api.ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = projectionLock(ctx, tx, m.TenantID, m.StreamID, true); err != nil {
		return err
	}
	var source bool
	err = tx.QueryRow(ctx, `SELECT e.event_type=$5 AND e.schema_version=$6 AND e.payload=$7::jsonb AND e.metadata=$8::jsonb FROM ordering.events e JOIN ordering.outbox o ON o.event_id=e.event_id WHERE e.event_id=$1::uuid AND e.tenant_id=$2 AND e.order_id=$3::uuid AND e.version=$4 FOR UPDATE OF o`, m.ID, m.TenantID, m.StreamID, m.Sequence, m.Type, m.SchemaVersion, []byte(m.Data), m.Metadata).Scan(&source)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !source {
		return api.ErrSource
	}
	if err != nil {
		return err
	}
	e := domain.Event{Version: m.Sequence, Type: m.Type, SchemaVersion: m.SchemaVersion, Data: m.Data}
	if err = applyEvent(ctx, tx, m.TenantID, m.StreamID, m.ID, e); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.outbox SET processed_at=coalesce(processed_at,clock_timestamp()),quarantined=false WHERE event_id=$1::uuid`, m.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func requestTable(topic string) (string, string, error) {
	switch topic {
	case api.CheckoutTopic:
		return "ordering.checkout_outbox", "checkout.requested", nil
	case api.FulfillmentTopic:
		return "ordering.fulfillment_outbox", "fulfillment.requested", nil
	}
	return "", "", api.ErrInvalid
}
func (s *Store) PublishCheckout(ctx context.Context, p api.Publisher) (bool, error) {
	return s.publishRequest(ctx, p, api.CheckoutTopic)
}
func (s *Store) PublishFulfillment(ctx context.Context, p api.Publisher) (bool, error) {
	return s.publishRequest(ctx, p, api.FulfillmentTopic)
}
func (s *Store) publishRequest(ctx context.Context, p api.Publisher, topic string) (bool, error) {
	table, kind, err := requestTable(topic)
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	m := api.Message{Version: 1, SchemaVersion: 1, Sequence: 1, Type: kind}
	err = tx.QueryRow(ctx, "SELECT tenant_id,id::text,order_id::text,body FROM "+table+" WHERE broker_published_at IS NULL AND dispatched_at IS NULL ORDER BY tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&m.TenantID, &m.ID, &m.StreamID, &m.Data)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if topic == api.CheckoutTopic {
		var r orders.Requested
		if err = json.Unmarshal(m.Data, &r); err != nil {
			return true, err
		}
		m.Metadata = r.Metadata
	} else {
		var r fulfill.Request
		if err = json.Unmarshal(m.Data, &r); err != nil {
			return true, err
		}
		m.Metadata = r.Metadata
	}
	if err = p.Publish(ctx, topic, m); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, "UPDATE "+table+" SET broker_published_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2::uuid", m.TenantID, m.ID); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) ValidateRequest(ctx context.Context, topic string, m api.Message) error {
	table, kind, err := requestTable(topic)
	if err != nil {
		return err
	}
	if !m.Valid(topic) || m.Sequence != 1 || m.SchemaVersion != 1 || m.Type != kind {
		return api.ErrInvalid
	}
	var same bool
	err = s.Pool.QueryRow(ctx, "SELECT body=$4::jsonb FROM "+table+" WHERE tenant_id=$1 AND id=$2::uuid AND order_id=$3::uuid", m.TenantID, m.ID, m.StreamID, []byte(m.Data)).Scan(&same)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !same {
		return api.ErrSource
	}
	return err
}
