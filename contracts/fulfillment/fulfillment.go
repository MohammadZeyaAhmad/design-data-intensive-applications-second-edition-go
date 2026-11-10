// Package fulfillment publishes confirmed-order requests without exposing the
// private shipment model or persistence. IDs are stable across redelivery.
package fulfillment

import "context"

type Allocation struct {
	ID          string `json:"id"`
	SKU         string `json:"sku"`
	WarehouseID string `json:"warehouse_id,omitempty"`
	Quantity    int64  `json:"quantity"`
}
type Request struct {
	ID          string       `json:"id"`
	TenantID    string       `json:"tenant_id"`
	OrderID     string       `json:"order_id"`
	Allocations []Allocation `json:"allocations"`
}
type Sink interface {
	Submit(context.Context, Request) error
}
type Dispatcher interface {
	DispatchFulfillmentOne(context.Context, Sink) (bool, error)
}
