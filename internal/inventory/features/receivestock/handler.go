package receivestock

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
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
	r, err := ports.NewRequest(tenant, "receive-stock", key, c)
	if err != nil {
		return ports.Outcome{}, false, err
	}
	return h.Store.Execute(ctx, r, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
		if err := tx.EnsureStock(ctx, c.SKU); err != nil {
			return ports.Outcome{}, err
		}
		s, err := tx.LockStock(ctx, c.SKU)
		if err != nil {
			return ports.Outcome{}, err
		}
		if err = s.Receive(c.Quantity); err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.SaveStock(ctx, s); err != nil {
			return ports.Outcome{}, err
		}
		out := ports.Outcome{Stock: s.Snapshot()}
		return out, tx.AppendEvent(ctx, "inventory.stock-received.v1", out)
	})
}
