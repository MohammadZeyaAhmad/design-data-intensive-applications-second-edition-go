package ports

import (
	"context"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Job struct {
	Metadata envelope.Metadata
	State    domain.State
	Token    int64
}
type View struct {
	Metadata         envelope.Metadata `json:"metadata"`
	DispatchMetadata envelope.Metadata `json:"dispatch_metadata,omitzero"`
	Paused           bool              `json:"paused"`
	ControlVersion   int64             `json:"control_version"`
	domain.State
	Attempts  int64  `json:"attempts"`
	LastError string `json:"last_error,omitempty"`
}
type Event struct {
	Metadata envelope.Metadata `json:"metadata"`
	Version  int64             `json:"version"`
	Action   string            `json:"action"`
	State    domain.State      `json:"state"`
}
type Store interface {
	api.Sink
	Execute(context.Context, commands.Request, string, int64, string, func(*domain.Shipment) error) (domain.State, bool, error)
	Get(context.Context, string, string) (View, error)
	ForOrder(context.Context, string, string) ([]View, error)
	History(context.Context, string, string) ([]Event, error)
	Claim(context.Context) (Job, bool, error)
	Save(context.Context, Job, bool) error
}
