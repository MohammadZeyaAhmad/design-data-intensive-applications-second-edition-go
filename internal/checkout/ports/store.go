package ports

import (
	"context"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
)

type Job struct {
	Metadata        envelope.Metadata
	Request         orders.Requested
	State           domain.State
	Token           int64
	CancelRequested bool
}
type View struct {
	Engine               string            `json:"engine"`
	Metadata             envelope.Metadata `json:"metadata"`
	CancellationMetadata envelope.Metadata `json:"cancellation_metadata,omitzero"`
	Paused               bool              `json:"paused"`
	ControlVersion       int64             `json:"control_version"`
	ID                   string            `json:"id"`
	OrderID              string            `json:"order_id"`
	Status               string            `json:"status"`
	Reason               string            `json:"reason,omitempty"`
	Currency             string            `json:"currency"`
	TotalMinor           int64             `json:"total_minor"`
	Attempts             int64             `json:"attempts"`
	CancelRequested      bool              `json:"cancel_requested"`
	UpdatedAt            time.Time         `json:"updated_at"`
}
type Store interface {
	orders.Sink
	Claim(context.Context) (Job, bool, error)
	Save(context.Context, Job, domain.State, bool) error
	Get(context.Context, string, string) (View, error)
	Cancel(context.Context, string, string) error
}
