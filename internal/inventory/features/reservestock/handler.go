package reservestock

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type Command struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (ports.Outcome, bool, error) {
	if !domain.ValidSKU(c.SKU) || !domain.ValidQuantity(c.Quantity) {
		return ports.Outcome{}, false, domain.ErrInvalidInput
	}
	r, err := ports.NewRequest(tenant, "reserve-stock", key, c)
	if err != nil {
		return ports.Outcome{}, false, err
	}
	return h.Store.Execute(ctx, r, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
		s, err := tx.LockStock(ctx, c.SKU)
		if err != nil {
			return ports.Outcome{}, err
		}
		if err = s.Reserve(c.Quantity); err != nil {
			return ports.Outcome{}, err
		}
		id, err := identity.NewID()
		if err != nil {
			return ports.Outcome{}, err
		}
		reservation, err := domain.NewReservation(id, tenant, c.SKU, c.Quantity)
		if err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.SaveStock(ctx, s); err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.InsertReservation(ctx, reservation); err != nil {
			return ports.Outcome{}, err
		}
		state := reservation.Snapshot()
		out := ports.Outcome{Stock: s.Snapshot(), Reservation: &state}
		return out, tx.AppendEvent(ctx, "inventory.stock-reserved.v1", out)
	})
}
