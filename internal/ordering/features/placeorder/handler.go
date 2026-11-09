package placeorder

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type Command struct {
	Lines []domain.Line `json:"lines"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (domain.State, bool, error) {
	if !domain.ValidLines(c.Lines) {
		return domain.State{}, false, domain.ErrInvalidInput
	}
	r, err := ports.NewRequest(tenant, "place-order", key, c)
	if err != nil {
		return domain.State{}, false, err
	}
	id, err := identity.NewID()
	if err != nil {
		return domain.State{}, false, err
	}
	return h.Store.Execute(ctx, r, id, 0, func(o *domain.Order) ([]domain.Event, error) { return o.Place(c.Lines) })
}
