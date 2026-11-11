package postgres

import (
	"context"
	"encoding/json"
	"strings"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
)

func projectionView(tenant, id string, version int64, body []byte) (api.Projection, error) {
	var state domain.State
	if json.Unmarshal(body, &state) != nil || state.TenantID != tenant || state.ID != id || state.Version != version {
		return api.Projection{}, domain.ErrInvalidEvent
	}
	if _, e := domain.Restore(state); e != nil {
		return api.Projection{}, e
	}
	out := api.Projection{TenantID: tenant, ID: id, Version: version, Status: state.Status, Lines: []api.ProjectionLine{}}
	prices := map[string]domain.PriceLine{}
	if state.Pricing != nil {
		out.Currency = state.Pricing.Currency
		for _, p := range state.Pricing.Lines {
			prices[p.SKU] = p
		}
	}
	for _, line := range state.Lines {
		l := api.ProjectionLine{SKU: line.SKU, Quantity: line.Quantity}
		if p, ok := prices[line.SKU]; ok {
			price, version := p.UnitPriceMinor, p.PriceVersion
			l.FrozenUnitPriceMinor = &price
			l.FrozenPriceVersion = &version
		}
		out.Lines = append(out.Lines, l)
	}
	return out, nil
}
func (s *Store) ReadOrderProjections(ctx context.Context, tenant string, ids []string) ([]api.Projection, error) {
	if !domain.ValidTenant(tenant) || len(ids) > 256 {
		return nil, domain.ErrInvalidInput
	}
	normalized := make([]string, len(ids))
	for i, id := range ids {
		if !domain.ValidID(id) {
			return nil, domain.ErrInvalidInput
		}
		normalized[i] = strings.ToLower(id)
	}
	out := []api.Projection{}
	if len(ids) == 0 {
		return out, ctx.Err()
	}
	rows, e := s.Pool.Query(ctx, `SELECT order_id::text,version,body FROM ordering.order_views WHERE tenant_id=$1 AND order_id=ANY($2::uuid[]) ORDER BY order_id`, tenant, normalized)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var version int64
		var body []byte
		if e = rows.Scan(&id, &version, &body); e != nil {
			return nil, e
		}
		p, e := projectionView(tenant, id, version, body)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) ListOrderProjections(ctx context.Context, tenant, after string, first int) (api.ProjectionPage, error) {
	out := api.ProjectionPage{Nodes: []api.Projection{}}
	if !domain.ValidTenant(tenant) || first < 1 || first > 20 || after != "" && !domain.ValidID(after) {
		return out, domain.ErrInvalidInput
	}
	// Empty cursor has a separate statement: no OR/cast of an empty UUID and no
	// offset scan. Both forms use the existing (tenant_id,order_id) primary key.
	query := `SELECT order_id::text,version,body FROM ordering.order_views WHERE tenant_id=$1 ORDER BY order_id LIMIT $2`
	args := []any{tenant, first + 1}
	if after != "" {
		query = `SELECT order_id::text,version,body FROM ordering.order_views WHERE tenant_id=$1 AND order_id>$3::uuid ORDER BY order_id LIMIT $2`
		args = append(args, after)
	}
	rows, e := s.Pool.Query(ctx, query, args...)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var version int64
		var body []byte
		if e = rows.Scan(&id, &version, &body); e != nil {
			return out, e
		}
		p, e := projectionView(tenant, id, version, body)
		if e != nil {
			return out, e
		}
		out.Nodes = append(out.Nodes, p)
	}
	if e = rows.Err(); e != nil {
		return out, e
	}
	if len(out.Nodes) > first {
		out.HasNextPage = true
		out.Nodes = out.Nodes[:first]
	}
	return out, nil
}
