package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres/detailsqlc"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
)

func contentHistory(ctx context.Context, tx pgx.Tx, tenant, kind, id string, version int64, value any) error {
	cause, e := identity.NewID()
	if e != nil {
		return e
	}
	m, e := envelope.Record(ctx, "system:internal", cause)
	if e != nil {
		return e
	}
	meta, e := json.Marshal(m)
	if e != nil {
		return e
	}
	body, e := json.Marshal(value)
	if e != nil {
		return e
	}
	schemaVersion := 1
	if kind == "brand" {
		brand, ok := value.(domain.Brand)
		if !ok {
			return domain.ErrInvalid
		}
		body, e = json.Marshal(api.BrandSnapshot{ID: brand.ID, DisplayName: brand.Name, Version: brand.Version})
		if e != nil {
			return e
		}
		schemaVersion = 2
	}
	_, e = tx.Exec(ctx, `INSERT INTO catalog.content_history(tenant_id,kind,id,version,body,metadata,schema_version) VALUES($1,$2,$3,$4,$5,$6,$7)`, tenant, kind, id, version, body, meta, schemaVersion)
	return e
}
func (s *Store) PublishBrand(ctx context.Context, r commands.Request, b domain.Brand) (domain.Brand, bool, error) {
	if !domain.ValidBrand(b) {
		return domain.Brand{}, false, domain.ErrInvalid
	}
	return cmdtx.Execute(ctx, s.Pool, "catalog", r, func(tx pgx.Tx) (domain.Brand, error) {
		var v int64
		e := tx.QueryRow(ctx, `INSERT INTO catalog.brands(tenant_id,id,display_name,version) VALUES($1,$2,$3,1) ON CONFLICT(tenant_id,id) DO UPDATE SET display_name=excluded.display_name,version=catalog.brands.version+1 WHERE catalog.brands.version=$4 RETURNING version`, r.Tenant, b.ID, b.Name, b.Version).Scan(&v)
		// A new brand may be created only with expected version zero.
		if errors.Is(e, pgx.ErrNoRows) {
			return domain.Brand{}, api.ErrConflict
		}
		if e != nil {
			return domain.Brand{}, e
		}
		if v == 1 && b.Version != 0 {
			return domain.Brand{}, api.ErrConflict
		}
		b.Version = v
		return b, contentHistory(ctx, tx, r.Tenant, "brand", b.ID, v, b)
	})
}
func (s *Store) Brand(ctx context.Context, tenant, id string) (domain.Brand, error) {
	var b domain.Brand
	e := s.Pool.QueryRow(ctx, `SELECT id,display_name,version FROM catalog.brand_names WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&b.ID, &b.Name, &b.Version)
	if errors.Is(e, pgx.ErrNoRows) {
		e = api.ErrNotFound
	}
	return b, e
}
func (s *Store) Describe(ctx context.Context, r commands.Request, d domain.Details) (domain.Details, bool, error) {
	if !domain.ValidDetails(d) {
		return domain.Details{}, false, domain.ErrInvalid
	}
	d.Attributes = append(json.RawMessage(nil), d.Attributes...)
	d.Categories = append([]string{}, d.Categories...)
	d.Variants = append([]domain.Variant{}, d.Variants...)
	sort.Strings(d.Categories)
	return cmdtx.Execute(ctx, s.Pool, "catalog", r, func(tx pgx.Tx) (domain.Details, error) {
		var source int64
		if e := tx.QueryRow(ctx, `SELECT version FROM catalog.products WHERE tenant_id=$1 AND sku=$2 FOR UPDATE`, r.Tenant, d.SKU).Scan(&source); errors.Is(e, pgx.ErrNoRows) {
			return domain.Details{}, api.ErrNotFound
		} else if e != nil {
			return domain.Details{}, e
		}
		if e := tx.QueryRow(ctx, `SELECT display_name FROM catalog.brand_names WHERE tenant_id=$1 AND id=$2 FOR SHARE`, r.Tenant, d.BrandID).Scan(&d.BrandName); errors.Is(e, pgx.ErrNoRows) {
			return domain.Details{}, api.ErrNotFound
		} else if e != nil {
			return domain.Details{}, e
		}
		var v int64
		e := tx.QueryRow(ctx, `SELECT version FROM catalog.product_details WHERE tenant_id=$1 AND sku=$2`, r.Tenant, d.SKU).Scan(&v)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return domain.Details{}, e
		}
		if v != d.Version {
			return domain.Details{}, api.ErrConflict
		}
		d.Version++
		if _, e = tx.Exec(ctx, `INSERT INTO catalog.product_details(tenant_id,sku,brand_id,description,attributes,version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,sku) DO UPDATE SET brand_id=excluded.brand_id,description=excluded.description,attributes=excluded.attributes,version=excluded.version`, r.Tenant, d.SKU, d.BrandID, d.Description, d.Attributes, d.Version); e != nil {
			return domain.Details{}, e
		}
		for _, table := range []string{"product_variants", "product_categories"} {
			if _, e = tx.Exec(ctx, `DELETE FROM catalog.`+table+` WHERE tenant_id=$1 AND sku=$2`, r.Tenant, d.SKU); e != nil {
				return domain.Details{}, e
			}
		}
		for i, v := range d.Variants {
			if _, e = tx.Exec(ctx, `INSERT INTO catalog.product_variants(tenant_id,sku,code,label,position) VALUES($1,$2,$3,$4,$5)`, r.Tenant, d.SKU, v.Code, v.Label, i); e != nil {
				return domain.Details{}, e
			}
		}
		for _, c := range d.Categories {
			if _, e = tx.Exec(ctx, `INSERT INTO catalog.product_categories(tenant_id,sku,category) VALUES($1,$2,$3)`, r.Tenant, d.SKU, c); e != nil {
				return domain.Details{}, e
			}
		}
		return d, contentHistory(ctx, tx, r.Tenant, "details", d.SKU, d.Version, d)
	})
}
func (s *Store) Details(ctx context.Context, tenant, sku string) (domain.Details, error) {
	r, e := detailsqlc.New(s.Pool).GetProductDetails(ctx, detailsqlc.GetProductDetailsParams{TenantID: tenant, Sku: sku})
	if errors.Is(e, pgx.ErrNoRows) {
		return domain.Details{}, api.ErrNotFound
	}
	if e != nil {
		return domain.Details{}, e
	}
	d := domain.Details{SKU: r.Sku, BrandID: r.BrandID, BrandName: r.BrandName, Description: r.Description, Attributes: r.Attributes, Version: r.Version, Categories: r.Categories}
	e = json.Unmarshal(r.Variants, &d.Variants)
	return d, e
}
