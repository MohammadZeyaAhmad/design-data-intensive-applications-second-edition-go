package ports

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
)

type Outcome struct {
	Stock       domain.StockState        `json:"stock"`
	Reservation *domain.ReservationState `json:"reservation,omitempty"`
}

type Request struct {
	TenantID  string
	Operation string
	Key       string
	Hash      [32]byte
}

func NewRequest(tenant, operation, key string, command any) (Request, error) {
	if !domain.ValidTenant(tenant) || len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key {
		return Request{}, domain.ErrInvalidInput
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return Request{}, domain.ErrInvalidInput
		}
	}
	b, err := json.Marshal(command)
	if err != nil {
		return Request{}, err
	}
	return Request{TenantID: tenant, Operation: operation, Key: key, Hash: sha256.Sum256(b)}, nil
}

// The transaction is scoped to one tenant and bounded context. Business rules
// execute in feature handlers against locked domain aggregates.
type Transaction interface {
	Now(context.Context) (time.Time, error)
	ManagedReservation(context.Context, string) (bool, error)
	EnsureStock(context.Context, string) error
	EnsureStockAt(context.Context, string, string) error
	LockStock(context.Context, string) (*domain.Stock, error)
	LockStockAt(context.Context, string, string) (*domain.Stock, error)
	SaveStock(context.Context, *domain.Stock) error
	LockReservation(context.Context, string) (*domain.Reservation, error)
	InsertReservation(context.Context, *domain.Reservation) error
	SaveReservation(context.Context, *domain.Reservation) error
	AppendEvent(context.Context, string, Outcome) error
}

type Store interface {
	Execute(context.Context, Request, func(context.Context, Transaction) (Outcome, error)) (Outcome, bool, error)
	Stock(context.Context, string, string) (domain.StockState, error)
	StockAt(context.Context, string, string, string) (domain.StockState, error)
	Projection(context.Context, string, string) (domain.StockState, error)
	ProjectionAt(context.Context, string, string, string) (domain.StockState, error)
	Reservation(context.Context, string, string) (domain.ReservationState, error)
}
