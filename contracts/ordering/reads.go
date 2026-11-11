package ordering

import "context"

// Projection is a published read contract. It carries no payment references or
// private event-store types and must never authorize a command transition.
type Projection struct {
	TenantID string
	ID       string
	Version  int64
	Status   string
	Currency string
	Lines    []ProjectionLine
}
type ProjectionLine struct {
	SKU                  string
	Quantity             int64
	FrozenUnitPriceMinor *int64
	FrozenPriceVersion   *int64
}
type ProjectionPage struct {
	Nodes       []Projection
	HasNextPage bool
}

type ProjectionReader interface {
	ReadOrderProjections(context.Context, string, []string) ([]Projection, error)
	ListOrderProjections(context.Context, string, string, int) (ProjectionPage, error)
}
