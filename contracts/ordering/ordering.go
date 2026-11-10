package ordering

import (
	"context"
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
)

var ErrConflict = errors.New("order changed or checkout transition not allowed")
var ErrInvalid = errors.New("quote or checkout input is invalid")
var ErrNotFound = errors.New("order not found")

type Requested struct {
	ID            string        `json:"id"`
	TenantID      string        `json:"tenant_id"`
	OrderID       string        `json:"order_id"`
	Quote         catalog.Quote `json:"quote"`
	PaymentMethod string        `json:"payment_method"`
}
type View struct {
	ID            string        `json:"id"`
	OrderID       string        `json:"order_id"`
	Status        string        `json:"status"`
	Version       int64         `json:"version"`
	Quote         catalog.Quote `json:"quote"`
	FailureReason string        `json:"failure_reason,omitempty"`
}
type Allocation struct {
	ID, SKU     string
	WarehouseID string
	Quantity    int64
}
type Proof struct {
	CheckoutID, PaymentID, Currency string
	AmountMinor                     int64
	Allocations                     []Allocation
}
type Service interface {
	Begin(context.Context, string, string, string, int64, catalog.Quote, string) (View, bool, error)
	Confirm(context.Context, string, string, Proof) (View, error)
	Fail(context.Context, string, string, string, string) (View, error)
}
type Sink interface {
	Submit(context.Context, Requested) error
}
type Dispatcher interface {
	DispatchOne(context.Context, Sink) (bool, error)
}
