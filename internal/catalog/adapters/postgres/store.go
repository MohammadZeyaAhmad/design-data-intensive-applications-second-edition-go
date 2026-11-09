package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

func (s *Store) Publish(ctx context.Context, r commands.Request, p domain.Product) (domain.Product, bool, error) {
	return cmdtx.Execute(ctx, s.Pool, "catalog", r, func(tx pgx.Tx) (domain.Product, error) {
		if _, err := tx.Exec(ctx, `INSERT INTO catalog.products(tenant_id,sku) VALUES($1,$2) ON CONFLICT DO NOTHING`, r.Tenant, p.SKU); err != nil {
			return domain.Product{}, err
		}
		var version int64
		if err := tx.QueryRow(ctx, `SELECT version FROM catalog.products WHERE tenant_id=$1 AND sku=$2 FOR UPDATE`, r.Tenant, p.SKU).Scan(&version); err != nil {
			return domain.Product{}, err
		}
		if version != p.Version {
			return domain.Product{}, catalog.ErrConflict
		}
		p.Version++
		if _, err := tx.Exec(ctx, `INSERT INTO catalog.prices(tenant_id,sku,version,name,unit_price_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, r.Tenant, p.SKU, p.Version, p.Name, p.UnitPriceMinor, p.Currency); err != nil {
			return domain.Product{}, err
		}
		_, err := tx.Exec(ctx, `UPDATE catalog.products SET version=$3 WHERE tenant_id=$1 AND sku=$2`, r.Tenant, p.SKU, p.Version)
		return p, err
	})
}
func (s *Store) Product(ctx context.Context, tenant, sku string) (domain.Product, error) {
	var p domain.Product
	err := s.Pool.QueryRow(ctx, `SELECT p.sku,p.name,p.unit_price_minor,p.currency,p.version FROM catalog.products h JOIN catalog.prices p ON p.tenant_id=h.tenant_id AND p.sku=h.sku AND p.version=h.version WHERE h.tenant_id=$1 AND h.sku=$2`, tenant, sku).Scan(&p.SKU, &p.Name, &p.UnitPriceMinor, &p.Currency, &p.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		err = catalog.ErrNotFound
	}
	return p, err
}
func (s *Store) CreateQuote(ctx context.Context, r commands.Request, lines []domain.Line) (catalog.Quote, bool, error) {
	return cmdtx.Execute(ctx, s.Pool, "catalog", r, func(tx pgx.Tx) (catalog.Quote, error) {
		// One SQL statement takes one MVCC snapshot for every requested price.
		skus := make([]string, len(lines))
		for i, l := range lines {
			skus[i] = l.SKU
		}
		rows, err := tx.Query(ctx, `SELECT p.sku,p.unit_price_minor,p.currency,p.version FROM catalog.products h JOIN catalog.prices p ON p.tenant_id=h.tenant_id AND p.sku=h.sku AND p.version=h.version WHERE h.tenant_id=$1 AND h.sku=ANY($2)`, r.Tenant, skus)
		if err != nil {
			return catalog.Quote{}, err
		}
		prices := map[string]domain.Product{}
		for rows.Next() {
			var p domain.Product
			if err = rows.Scan(&p.SKU, &p.UnitPriceMinor, &p.Currency, &p.Version); err != nil {
				rows.Close()
				return catalog.Quote{}, err
			}
			prices[p.SKU] = p
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return catalog.Quote{}, err
		}
		id, err := identity.NewID()
		if err != nil {
			return catalog.Quote{}, err
		}
		q := catalog.Quote{ID: id, TenantID: r.Tenant}
		for _, l := range lines {
			p, ok := prices[l.SKU]
			if !ok {
				return catalog.Quote{}, catalog.ErrNotFound
			}
			if q.Currency != "" && q.Currency != p.Currency {
				return catalog.Quote{}, catalog.ErrInvalid
			}
			q.Currency = p.Currency
			q.TotalMinor, err = domain.Add(q.TotalMinor, p.UnitPriceMinor, l.Quantity)
			if err != nil {
				return catalog.Quote{}, err
			}
			q.Lines = append(q.Lines, catalog.PriceLine{SKU: l.SKU, Quantity: l.Quantity, UnitPriceMinor: p.UnitPriceMinor, PriceVersion: p.Version})
		}
		if err = tx.QueryRow(ctx, `SELECT clock_timestamp()+interval '5 minutes'`).Scan(&q.ExpiresAt); err != nil {
			return catalog.Quote{}, err
		}
		q.ExpiresAt = q.ExpiresAt.UTC()
		b, err := json.Marshal(q)
		if err != nil {
			return catalog.Quote{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO catalog.quotes(tenant_id,id,body,expires_at) VALUES($1,$2::uuid,$3,$4)`, r.Tenant, q.ID, b, q.ExpiresAt)
		return q, err
	})
}
func (s *Store) Quote(ctx context.Context, tenant, id string) (catalog.Quote, error) {
	var body []byte
	err := s.Pool.QueryRow(ctx, `SELECT body FROM catalog.quotes WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.Quote{}, catalog.ErrNotFound
	}
	if err != nil {
		return catalog.Quote{}, err
	}
	var q catalog.Quote
	err = json.Unmarshal(body, &q)
	return q, err
}
