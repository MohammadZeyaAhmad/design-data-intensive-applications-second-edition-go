package ports

import (
	"context"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
)

type ViewQuery struct{ SKU, WarehouseID, ReservationID string }
type View struct {
	Stock       *domain.StockState       `json:"stock,omitempty"`
	Reservation *domain.ReservationState `json:"reservation,omitempty"`
}
type ReadOptions struct {
	Token, Mode, OnLag string
	Wait               time.Duration
}
type ReadMeta struct{ Token, Source, Reason string }
type SessionReader interface {
	SessionEnabled() bool
	VerifySession(context.Context, string, string) error
	AfterWrite(context.Context, string, string) (ReadMeta, error)
	QueryView(context.Context, string, ViewQuery, ReadOptions) (View, ReadMeta, error)
}
