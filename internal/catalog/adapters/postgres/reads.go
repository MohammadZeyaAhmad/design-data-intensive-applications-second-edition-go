package postgres

import (
	"context"
	"encoding/json"
	"regexp"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres/detailsqlc"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
)

var readTenantName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func validReadBatch(tenant string, skus []string) bool {
	if !readTenantName.MatchString(tenant) || len(skus) > 2000 {
		return false
	}
	for _, sku := range skus {
		if !domain.ValidLines([]domain.Line{{SKU: sku, Quantity: 1}}) {
			return false
		}
	}
	return true
}
func (s *Store) ReadProducts(ctx context.Context, tenant string, skus []string) ([]api.ProductView, error) {
	if !validReadBatch(tenant, skus) {
		return nil, api.ErrInvalid
	}
	out := []api.ProductView{}
	if len(skus) == 0 {
		return out, ctx.Err()
	}
	rows, e := s.Pool.Query(ctx, `SELECT p.sku,p.name,p.unit_price_minor,p.currency,p.version FROM catalog.products h JOIN catalog.prices p ON p.tenant_id=h.tenant_id AND p.sku=h.sku AND p.version=h.version WHERE h.tenant_id=$1 AND h.sku=ANY($2::text[]) ORDER BY p.sku`, tenant, skus)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		p := api.ProductView{TenantID: tenant}
		if e = rows.Scan(&p.SKU, &p.Name, &p.UnitPriceMinor, &p.Currency, &p.Version); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) ReadProductDetails(ctx context.Context, tenant string, skus []string) ([]api.DetailView, error) {
	if !validReadBatch(tenant, skus) {
		return nil, api.ErrInvalid
	}
	out := []api.DetailView{}
	if len(skus) == 0 {
		return out, ctx.Err()
	}
	rows, e := detailsqlc.New(s.Pool).ListProductDetails(ctx, detailsqlc.ListProductDetailsParams{TenantID: tenant, Column2: skus})
	if e != nil {
		return nil, e
	}
	for _, r := range rows {
		d := api.DetailView{TenantID: tenant, SKU: r.Sku, Description: r.Description, Version: r.Version, Brand: api.BrandView{TenantID: tenant, ID: r.BrandID, Name: r.BrandName}, Categories: r.Categories, Variants: []api.VariantView{}}
		if e = json.Unmarshal(r.Variants, &d.Variants); e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, nil
}
