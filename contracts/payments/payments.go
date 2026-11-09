package payments

import (
	"context"
	"errors"
)

var ErrUnknown = errors.New("provider outcome is uncertain")
var ErrNotFound = errors.New("payment not found")
var ErrConflict = errors.New("payment identity has different amount or currency")

type Intent struct {
	TenantID    string `json:"tenant_id"`
	ID          string `json:"id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Method      string `json:"method,omitempty"`
}
type Receipt struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}
type Service interface {
	Capture(context.Context, Intent) (Receipt, error)
	Close(context.Context, Intent) (Receipt, error)
	Get(context.Context, string, string) (Receipt, error)
}
