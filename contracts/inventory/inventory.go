package inventory

import (
	"context"
	"errors"
	"time"
)

var ErrUnavailable = errors.New("inventory unavailable or hold expired")
var ErrClosed = errors.New("inventory step is closed")

type Line struct {
	SKU      string
	Quantity int64
}
type Hold struct {
	ID, SKU, Status, WarehouseID string
	Quantity                     int64
	ExpiresAt                    time.Time
}

// Step is a stable workflow operation ID. Close also fences Reserve calls that
// have not yet arrived; a successful compensation cannot recreate a hold.
type Service interface {
	Reserve(context.Context, string, string, Line, int64) (Hold, error)
	Commit(context.Context, string, string) (Hold, error)
	Get(context.Context, string, string) (Hold, error)
	Close(context.Context, string, string, Line) error
}

// Dispatcher is owned by Fulfillment after the source order confirms. It removes
// committed physical stock once per allocation; retries return the shipped hold.
type Dispatcher interface {
	Dispatch(context.Context, string, string) (Hold, error)
}
