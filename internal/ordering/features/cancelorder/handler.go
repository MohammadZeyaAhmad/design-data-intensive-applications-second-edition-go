package cancelorder

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
)

type Command struct {
	ID              string `json:"id"`
	ExpectedVersion int64  `json:"expected_version"`
	Reason          string `json:"reason"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (domain.State, bool, error) {
	if !domain.ValidID(c.ID) || c.ExpectedVersion < 1 {
		return domain.State{}, false, domain.ErrInvalidInput
	}
	r, err := ports.NewRequest(tenant, "cancel-order", key, c)
	if err != nil {
		return domain.State{}, false, err
	}
	return h.Store.Execute(ctx, r, c.ID, c.ExpectedVersion, func(o *domain.Order) ([]domain.Event, error) { return o.Cancel(c.Reason) })
}
