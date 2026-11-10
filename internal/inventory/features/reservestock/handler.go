package reservestock

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"time"
)

type Command struct {
	WarehouseID string `json:"warehouse_id,omitempty"`
	SKU         string `json:"sku"`
	Quantity    int64  `json:"quantity"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (ports.Outcome, bool, error) {
	if !domain.ValidWarehouse(c.WarehouseID) || !domain.ValidSKU(c.SKU) || !domain.ValidQuantity(c.Quantity) || c.TTLSeconds < 0 || c.TTLSeconds > 3600 {
		return ports.Outcome{}, false, domain.ErrInvalidInput
	}
	if c.WarehouseID == "default" {
		c.WarehouseID = ""
	}
	r, err := ports.NewRequest(tenant, "reserve-stock", key, c)
	if err != nil {
		return ports.Outcome{}, false, err
	}
	return h.Store.Execute(ctx, r, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
		s, err := tx.LockStockAt(ctx, c.SKU, c.WarehouseID)
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
		now, err := tx.Now(ctx)
		if err != nil {
			return ports.Outcome{}, err
		}
		ttl := c.TTLSeconds
		if ttl == 0 {
			ttl = 900
		}
		reservation, err := domain.NewReservationAt(id, tenant, c.SKU, c.WarehouseID, c.Quantity, now.Add(time.Duration(ttl)*time.Second))
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
