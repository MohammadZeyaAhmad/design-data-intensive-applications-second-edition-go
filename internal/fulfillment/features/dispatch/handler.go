package dispatch

import (
	"context"
	"errors"
	inv "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
)

type Handler struct{ Inventory inv.Dispatcher }

func (h Handler) Handle(ctx context.Context, j ports.Job) error {
	s := j.State
	if s.Status != domain.Dispatching || s.Dispatched < 0 || s.Dispatched >= len(s.Lines) {
		return domain.ErrInvalid
	}
	a := s.Lines[s.Dispatched]
	r, err := h.Inventory.Dispatch(ctx, s.TenantID, a.ID)
	if err != nil {
		return err
	}
	if r.ID != a.ID || r.SKU != a.SKU || r.Quantity != a.Quantity || domain.Warehouse(r.WarehouseID) != s.WarehouseID || r.Status != "shipped" {
		return errors.New("inventory shipment receipt mismatch")
	}
	return nil
}
