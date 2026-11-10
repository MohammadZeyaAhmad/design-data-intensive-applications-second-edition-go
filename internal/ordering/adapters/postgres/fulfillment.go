package postgres

import (
	"context"
	"encoding/json"
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/jackc/pgx/v5"
)

var _ api.Dispatcher = (*Store)(nil)

func enqueueFulfillment(ctx context.Context, tx pgx.Tx, tenant, order string, e domain.Event) error {
	var proof domain.PaymentProof
	if err := json.Unmarshal(e.Data, &proof); err != nil {
		return err
	}
	r := api.Request{ID: proof.CheckoutID, TenantID: tenant, OrderID: order}
	for _, a := range proof.Allocations {
		r.Allocations = append(r.Allocations, api.Allocation{ID: a.ID, SKU: a.SKU, WarehouseID: a.WarehouseID, Quantity: a.Quantity})
	}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ordering.fulfillment_outbox(tenant_id,id,order_id,body) VALUES($1,$2::uuid,$3::uuid,$4)`, tenant, r.ID, order, body)
	return err
}
func (s *Store) DispatchFulfillmentOne(ctx context.Context, sink api.Sink) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var body []byte
	var tenant, id string
	err = tx.QueryRow(ctx, `SELECT tenant_id,id::text,body FROM ordering.fulfillment_outbox WHERE dispatched_at IS NULL ORDER BY tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&tenant, &id, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var r api.Request
	if err = json.Unmarshal(body, &r); err != nil {
		return true, err
	}
	if r.ID != id || r.TenantID != tenant {
		return true, domain.ErrInvalidEvent
	}
	if err = sink.Submit(ctx, r); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.fulfillment_outbox SET dispatched_at=now() WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
