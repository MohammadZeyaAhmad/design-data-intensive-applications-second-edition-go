package changeshipment

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Command struct {
	ExpectedVersion   int64  `json:"expected_version"`
	TrackingReference string `json:"tracking_reference,omitempty"`
}
type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key, id, action string, c Command) (domain.State, bool, error) {
	if !domain.ValidID(id) || c.ExpectedVersion < 1 {
		return domain.State{}, false, domain.ErrInvalid
	}
	payload := struct {
		ID, Action string
		Command
	}{id, action, c}
	r, err := commands.New(tenant, "shipment-"+action, key, payload)
	if err != nil {
		return domain.State{}, false, err
	}
	return h.Store.Execute(ctx, r, id, c.ExpectedVersion, action, func(s *domain.Shipment) error { return s.Change(action, c.TrackingReference) })
}
