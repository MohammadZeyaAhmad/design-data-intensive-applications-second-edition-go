package postgres

import (
	"context"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
)

var _ api.Dispatcher = Workflow{}

func (w Workflow) Dispatch(ctx context.Context, tenant, id string) (api.Hold, error) {
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
	if state.Status == domain.ReservationShipped {
		return holdView(state), tx.Commit(ctx)
	}
	s, err := t.LockStockAt(ctx, state.SKU, state.WarehouseID)
	if err != nil {
		return api.Hold{}, err
	}
	now, err := t.Now(ctx)
	if err != nil {
		return api.Hold{}, err
	}
	if _, err = r.Transition(domain.ReservationShipped, now); err != nil {
		return api.Hold{}, workflowError(err)
	}
	if err = s.Ship(state.Quantity); err != nil {
		return api.Hold{}, err
	}
	if err = t.SaveStock(ctx, s); err != nil {
		return api.Hold{}, err
	}
	if err = t.SaveReservation(ctx, r); err != nil {
		return api.Hold{}, err
	}
	state = r.Snapshot()
	if err = t.AppendEvent(ctx, "inventory.allocation-shipped.v1", ports.Outcome{Stock: s.Snapshot(), Reservation: &state}); err != nil {
		return api.Hold{}, err
	}
	return holdView(state), tx.Commit(ctx)
}
