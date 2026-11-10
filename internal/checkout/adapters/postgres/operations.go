package postgres

import (
	"context"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/workcontrol"
)

var _ api.Workflow = (*Store)(nil)

func (s *Store) controls() workcontrol.Store {
	return workcontrol.Store{Pool: s.Pool, Schema: "checkout", Table: "jobs", Kind: "checkout", Terminal: []string{"confirmed", "failed", "cancelled"}}
}
func (s *Store) Inspect(ctx context.Context, tenant, id string) (api.View, error) {
	return s.controls().Inspect(ctx, tenant, id)
}
func (s *Store) Control(ctx context.Context, c api.Command) (api.Result, bool, error) {
	return s.controls().Control(ctx, c)
}
func (s *Store) ControlHistory(ctx context.Context, tenant, id string, after int64, limit int) (api.Page, error) {
	return s.controls().ControlHistory(ctx, tenant, id, after, limit)
}
