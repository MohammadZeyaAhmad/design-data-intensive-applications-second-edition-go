package changereservation

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type Command struct {
	ID     string `json:"id"`
	Target string `json:"target"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (ports.Outcome, bool, error) {
	if !identity.ValidID(c.ID) || (c.Target != domain.ReservationCommitted && c.Target != domain.ReservationReleased) {
		return ports.Outcome{}, false, domain.ErrInvalidInput
	}
	r, err := ports.NewRequest(tenant, "change-reservation", key, c)
	if err != nil {
		return ports.Outcome{}, false, err
	}
	return h.Store.Execute(ctx, r, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
		reservation, err := tx.LockReservation(ctx, c.ID)
		if err != nil {
			return ports.Outcome{}, err
		}
		changed, err := reservation.Transition(c.Target)
		if err != nil {
			return ports.Outcome{}, err
		}
		state := reservation.Snapshot()
		s, err := tx.LockStock(ctx, state.SKU)
		if err != nil {
			return ports.Outcome{}, err
		}
		if !changed {
			return ports.Outcome{Stock: s.Snapshot(), Reservation: &state}, nil
		}
		event := "inventory.reservation-released.v1"
		if c.Target == domain.ReservationCommitted {
			err = s.Commit(state.Quantity)
			event = "inventory.reservation-committed.v1"
		} else {
			err = s.Release(state.Quantity)
		}
		if err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.SaveStock(ctx, s); err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.SaveReservation(ctx, reservation); err != nil {
			return ports.Outcome{}, err
		}
		out := ports.Outcome{Stock: s.Snapshot(), Reservation: &state}
		return out, tx.AppendEvent(ctx, event, out)
	})
}
