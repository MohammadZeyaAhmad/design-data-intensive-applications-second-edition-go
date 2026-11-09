package ports

import (
	"context"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"time"
)

type Job struct {
	Request         orders.Requested
	State           domain.State
	Token           int64
	CancelRequested bool
}
type View struct {
	ID              string    `json:"id"`
	OrderID         string    `json:"order_id"`
	Status          string    `json:"status"`
	Reason          string    `json:"reason,omitempty"`
	Currency        string    `json:"currency"`
	TotalMinor      int64     `json:"total_minor"`
	Attempts        int64     `json:"attempts"`
	CancelRequested bool      `json:"cancel_requested"`
	UpdatedAt       time.Time `json:"updated_at"`
}
type Store interface {
	orders.Sink
	Claim(context.Context) (Job, bool, error)
	Save(context.Context, Job, domain.State, bool) error
	Get(context.Context, string, string) (View, error)
	Cancel(context.Context, string, string) error
}
