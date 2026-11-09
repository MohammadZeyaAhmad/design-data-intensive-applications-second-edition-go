package start

import (
	"context"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
)

type Handler struct {
	Catalog cat.Reader
	Orders  orders.Service
}
type Command struct {
	OrderID                string
	ExpectedVersion        int64
	QuoteID, PaymentMethod string
}

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (orders.View, bool, error) {
	if !ValidMethod(c.PaymentMethod) {
		return orders.View{}, false, orders.ErrInvalid
	}
	q, err := h.Catalog.Quote(ctx, tenant, c.QuoteID)
	if err != nil {
		return orders.View{}, false, err
	}
	return h.Orders.Begin(ctx, tenant, c.OrderID, key, c.ExpectedVersion, q, c.PaymentMethod)
}

func ValidMethod(s string) bool {
	return s == "test-ok" || s == "test-decline" || s == "test-lost-response" || s == "test-slow" || s == "test-refund-lost-response"
}
