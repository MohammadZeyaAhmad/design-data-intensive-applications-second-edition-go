package ports

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
)

type Request struct {
	Tenant, Operation, Key string
	Hash                   [32]byte
}

func NewRequest(tenant, operation, key string, payload any) (Request, error) {
	if !domain.ValidTenant(tenant) || len(key) < 1 || len(key) > 128 {
		return Request{}, domain.ErrInvalidInput
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return Request{}, domain.ErrInvalidInput
		}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Request{}, err
	}
	return Request{Tenant: tenant, Operation: operation, Key: key, Hash: sha256.Sum256(b)}, nil
}

type EventRecord struct {
	ID string `json:"event_id"`
	domain.Event
	RecordedAt time.Time `json:"recorded_at"`
}
type EventPage struct {
	Events           []EventRecord `json:"events"`
	NextAfterVersion int64         `json:"next_after_version"`
}
type Store interface {
	Execute(context.Context, Request, string, int64, func(*domain.Order) ([]domain.Event, error)) (domain.State, bool, error)
	Load(context.Context, string, string) (domain.State, error)
	Projection(context.Context, string, string) (domain.State, error)
	Events(context.Context, string, string, int64, int) (EventPage, error)
}
