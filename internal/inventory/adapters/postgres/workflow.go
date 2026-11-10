package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
)

type Workflow struct{ Store *Store }

var _ api.Service = Workflow{}

func holdView(r domain.ReservationState) api.Hold {
	return api.Hold{ID: r.ID, SKU: r.SKU, WarehouseID: r.WarehouseID, Quantity: r.Quantity, Status: r.Status, ExpiresAt: r.ExpiresAt}
}
func workflowError(err error) error {
	if errors.Is(err, domain.ErrInsufficientStock) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrReservationExpired) || errors.Is(err, domain.ErrInvalidTransition) {
		return api.ErrUnavailable
	}
	return err
}
func lockStep(ctx context.Context, tx pgx.Tx, tenant, id string, line api.Line, create bool) (bool, error) {
	if !domain.ValidTenant(tenant) || !identity.ValidID(id) {
		return false, domain.ErrInvalidInput
	}
	if create {
		if !domain.ValidSKU(line.SKU) || !domain.ValidQuantity(line.Quantity) {
			return false, domain.ErrInvalidInput
		}
		if _, err := tx.Exec(ctx, `INSERT INTO inventory.workflow_steps(tenant_id,id,sku,quantity) VALUES($1,$2::uuid,$3,$4) ON CONFLICT DO NOTHING`, tenant, id, line.SKU, line.Quantity); err != nil {
			return false, err
		}
	}
	var sku string
	var quantity int64
	var closed bool
	err := tx.QueryRow(ctx, `SELECT sku,quantity,closed FROM inventory.workflow_steps WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, tenant, id).Scan(&sku, &quantity, &closed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, api.ErrUnavailable
	}
	if err != nil {
		return false, err
	}
	if create && (sku != line.SKU || quantity != line.Quantity) {
		return false, domain.ErrInvalidInput
	}
	return closed, nil
}
func (w Workflow) Reserve(ctx context.Context, tenant, id string, line api.Line, ttl int64) (api.Hold, error) {
	metadata, metaErr := envelope.Record(ctx, "system:internal", id)
	if metaErr != nil {
		return api.Hold{}, metaErr
	}
	ctx = envelope.With(ctx, metadata)
	if ttl < 1 || ttl > 3600 {
		return api.Hold{}, domain.ErrInvalidInput
	}
	tx, err := w.Store.Pool.Begin(ctx)
	if err != nil {
		return api.Hold{}, err
	}
	defer rollback(tx)
	closed, err := lockStep(ctx, tx, tenant, id, line, true)
	if err != nil {
		return api.Hold{}, err
	}
	if closed {
		return api.Hold{}, api.ErrClosed
	}
	t := &transaction{tx: tx, tenant: tenant}
	existing, err := t.LockReservation(ctx, id)
	if err == nil {
		r := existing.Snapshot()
		if r.Status == domain.ReservationReleased || r.Status == domain.ReservationExpired || r.Status == domain.ReservationReversed || r.Status == domain.ReservationShipped {
			return api.Hold{}, api.ErrClosed
		}
		now, clockErr := t.Now(ctx)
		if clockErr != nil {
			return api.Hold{}, clockErr
		}
		if r.Status == domain.ReservationActive && !now.Before(r.ExpiresAt) {
			return api.Hold{}, api.ErrUnavailable
		}
		return holdView(r), tx.Commit(ctx)
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return api.Hold{}, err
	}
	// A line is sourced wholly from one location. Row locking rechecks
	// availability after waits; separate warehouses have independent streams.
	stockState, err := scanStock(tx.QueryRow(ctx, `SELECT tenant_id,sku,on_hand,reserved,allocated,version,warehouse_id FROM inventory.stock WHERE tenant_id=$1 AND sku=$2 AND on_hand-reserved-allocated >= $3 ORDER BY (warehouse_id='default') DESC,warehouse_id LIMIT 1 FOR UPDATE`, tenant, line.SKU, line.Quantity))
	var s *domain.Stock
	if err == nil {
		s, err = domain.RestoreStock(stockState)
	}
	if err != nil {
		return api.Hold{}, workflowError(err)
	}
	if err = s.Reserve(line.Quantity); err != nil {
		return api.Hold{}, workflowError(err)
	}
	now, err := t.Now(ctx)
	if err != nil {
		return api.Hold{}, err
	}
	r, err := domain.NewReservationAt(id, tenant, line.SKU, s.Snapshot().WarehouseID, line.Quantity, now.Add(time.Duration(ttl)*time.Second))
	if err != nil {
		return api.Hold{}, err
	}
	if err = t.SaveStock(ctx, s); err != nil {
		return api.Hold{}, err
	}
	if err = t.InsertReservation(ctx, r); err != nil {
		return api.Hold{}, err
	}
	state := r.Snapshot()
	if err = t.AppendEvent(ctx, "inventory.stock-reserved.v1", ports.Outcome{Stock: s.Snapshot(), Reservation: &state}); err != nil {
		return api.Hold{}, err
	}
	return holdView(state), tx.Commit(ctx)
}
func (w Workflow) Get(ctx context.Context, tenant, id string) (api.Hold, error) {
	r, err := w.Store.Reservation(ctx, tenant, id)
	return holdView(r), workflowError(err)
}
func (w Workflow) Commit(ctx context.Context, tenant, id string) (api.Hold, error) {
	metadata, metaErr := envelope.Record(ctx, "system:internal", id)
	if metaErr != nil {
		return api.Hold{}, metaErr
	}
	ctx = envelope.With(ctx, metadata)
	tx, err := w.Store.Pool.Begin(ctx)
	if err != nil {
		return api.Hold{}, err
	}
	defer rollback(tx)
	closed, err := lockStep(ctx, tx, tenant, id, api.Line{}, false)
	if err != nil {
		return api.Hold{}, err
	}
	if closed {
		return api.Hold{}, api.ErrClosed
	}
	t := &transaction{tx: tx, tenant: tenant}
	r, err := t.LockReservation(ctx, id)
	if err != nil {
		return api.Hold{}, workflowError(err)
	}
	state := r.Snapshot()
	s, err := t.LockStockAt(ctx, state.SKU, state.WarehouseID)
	if err != nil {
		return api.Hold{}, err
	}
	now, err := t.Now(ctx)
	if err != nil {
		return api.Hold{}, err
	}
	changed, err := r.Transition(domain.ReservationCommitted, now)
	if err != nil {
		return api.Hold{}, workflowError(err)
	}
	state = r.Snapshot()
	if changed {
		if err = s.Commit(state.Quantity); err != nil {
			return api.Hold{}, err
		}
		if err = t.SaveStock(ctx, s); err != nil {
			return api.Hold{}, err
		}
		if err = t.SaveReservation(ctx, r); err != nil {
			return api.Hold{}, err
		}
		if err = t.AppendEvent(ctx, "inventory.reservation-committed.v1", ports.Outcome{Stock: s.Snapshot(), Reservation: &state}); err != nil {
			return api.Hold{}, err
		}
	}
	return holdView(state), tx.Commit(ctx)
}
func (w Workflow) Close(ctx context.Context, tenant, id string, line api.Line) error {
	metadata, metaErr := envelope.Record(ctx, "system:internal", id)
	if metaErr != nil {
		return metaErr
	}
	ctx = envelope.With(ctx, metadata)
	tx, err := w.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	_, err = lockStep(ctx, tx, tenant, id, line, true)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE inventory.workflow_steps SET closed=true WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id); err != nil {
		return err
	}
	t := &transaction{tx: tx, tenant: tenant}
	r, err := t.LockReservation(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	state := r.Snapshot()
	if state.Status == domain.ReservationShipped {
		return api.ErrClosed
	}
	if state.Status != domain.ReservationActive && state.Status != domain.ReservationCommitted {
		return tx.Commit(ctx)
	}
	s, err := t.LockStockAt(ctx, state.SKU, state.WarehouseID)
	if err != nil {
		return err
	}
	now, err := t.Now(ctx)
	if err != nil {
		return err
	}
	target, event := domain.ReservationReleased, "inventory.reservation-released.v1"
	if state.Status == domain.ReservationCommitted {
		target = domain.ReservationReversed
		event = "inventory.allocation-reversed.v1"
		err = s.ReverseAllocation(state.Quantity)
	} else {
		err = s.Release(state.Quantity)
	}
	if err != nil {
		return err
	}
	if _, err = r.Transition(target, now); err != nil {
		return err
	}
	state = r.Snapshot()
	if err = t.SaveStock(ctx, s); err != nil {
		return err
	}
	if err = t.SaveReservation(ctx, r); err != nil {
		return err
	}
	if err = t.AppendEvent(ctx, event, ports.Outcome{Stock: s.Snapshot(), Reservation: &state}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
