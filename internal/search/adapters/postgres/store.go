package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool          *pgxpool.Pool
	VectorEnabled bool
}

func provenance(ctx context.Context) ([]byte, error) {
	id, e := identity.NewID()
	if e != nil {
		return nil, e
	}
	m, e := envelope.Record(ctx, "system:internal", id)
	if e != nil {
		return nil, e
	}
	return json.Marshal(m)
}
func (s *Store) Publish(ctx context.Context, r commands.Request, d domain.Document) (domain.Document, bool, error) {
	if !d.Valid() {
		return domain.Document{}, false, domain.ErrInvalid
	}
	return s.change(ctx, r, d)
}
func (s *Store) Remove(ctx context.Context, r commands.Request, id string, expected int64) (domain.Document, bool, error) {
	if !domain.ValidID(id) || expected < 0 || expected >= 1e12 {
		return domain.Document{}, false, domain.ErrInvalid
	}
	return s.change(ctx, r, domain.Document{ID: id, Version: expected, Deleted: true})
}
func (s *Store) change(ctx context.Context, r commands.Request, d domain.Document) (domain.Document, bool, error) {
	return cmdtx.Execute(ctx, s.Pool, "search", r, func(tx pgx.Tx) (domain.Document, error) {
		if _, e := tx.Exec(ctx, `INSERT INTO search.documents(tenant_id,id,version,deleted) VALUES($1,$2,0,true) ON CONFLICT DO NOTHING`, r.Tenant, d.ID); e != nil {
			return domain.Document{}, e
		}
		var version int64
		if e := tx.QueryRow(ctx, `SELECT version FROM search.documents WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, r.Tenant, d.ID).Scan(&version); e != nil {
			return domain.Document{}, e
		}
		if version != d.Version {
			return domain.Document{}, domain.ErrConflict
		}
		d.Version++
		data, e := json.Marshal(d)
		if e != nil {
			return domain.Document{}, e
		}
		meta, e := provenance(ctx)
		if e != nil {
			return domain.Document{}, e
		}
		var lon, lat *float64
		if d.Point != nil {
			lon = &d.Point.Lon
			lat = &d.Point.Lat
		}
		if _, e = tx.Exec(ctx, `UPDATE search.documents SET version=$3,sku=$4,warehouse=$5,title=$6,body=$7,longitude=$8,latitude=$9,deleted=$10,data=$11,metadata=$12 WHERE tenant_id=$1 AND id=$2`, r.Tenant, d.ID, d.Version, d.SKU, d.Warehouse, d.Title, d.Body, lon, lat, d.Deleted, data, meta); e != nil {
			return domain.Document{}, e
		}
		if _, e = tx.Exec(ctx, `INSERT INTO search.document_history(tenant_id,id,version,body,metadata) VALUES($1,$2,$3,$4,$5)`, r.Tenant, d.ID, d.Version, data, meta); e != nil {
			return domain.Document{}, e
		}
		return d, nil
	})
}
func (s *Store) Document(ctx context.Context, tenant, id string) (domain.Document, error) {
	if !domain.ValidID(id) {
		return domain.Document{}, domain.ErrInvalid
	}
	var data []byte
	e := s.Pool.QueryRow(ctx, `SELECT data FROM search.documents WHERE tenant_id=$1 AND id=$2 AND NOT deleted`, tenant, id).Scan(&data)
	if errors.Is(e, pgx.ErrNoRows) {
		return domain.Document{}, domain.ErrNotFound
	}
	if e != nil {
		return domain.Document{}, e
	}
	var d domain.Document
	e = json.Unmarshal(data, &d)
	return d, e
}
func collect(rows pgx.Rows, e error) ([]api.Hit, error) {
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []api.Hit
	for rows.Next() {
		var h api.Hit
		if e = rows.Scan(&h.ID, &h.SKU, &h.Warehouse, &h.Title, &h.Version, &h.Rank, &h.Distance, &h.EmbeddingVersion); e != nil {
			return nil, e
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

const hitColumns = `id,sku,warehouse,title,version`

func (s *Store) Text(ctx context.Context, tenant, q string, k int) ([]api.Hit, error) {
	if !domain.ValidText(q, k) {
		return nil, domain.ErrInvalid
	}
	return collect(s.Pool.Query(ctx, `SELECT `+hitColumns+`,ts_rank_cd(terms,websearch_to_tsquery('english',$2),32)::float8,0::float8,0::bigint FROM search.documents WHERE tenant_id=$1 AND NOT deleted AND terms @@ websearch_to_tsquery('english',$2) ORDER BY 6 DESC,id LIMIT $3`, tenant, q, k))
}
func (s *Store) Suggest(ctx context.Context, tenant, q string, k int) ([]api.Hit, error) {
	if !domain.ValidText(q, k) {
		return nil, domain.ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer cmdtx.Rollback(tx)
	if _, e = tx.Exec(ctx, `SET LOCAL pg_trgm.similarity_threshold='0.25'`); e != nil {
		return nil, e
	}
	return collect(tx.Query(ctx, `SELECT `+hitColumns+`,similarity(title,$2)::float8,0::float8,0::bigint FROM search.documents WHERE tenant_id=$1 AND NOT deleted AND title % $2 ORDER BY 6 DESC,id LIMIT $3`, tenant, q, k))
}
func boxClause(wrap bool) string {
	if !wrap {
		return `location <@ box(point($2,$3),point($4,$5))`
	}
	return `(location <@ box(point($2,$3),point(180,$5)) OR location <@ box(point(-180,$3),point($4,$5)))`
}
func (s *Store) Box(ctx context.Context, tenant string, b domain.Box, k int) ([]api.Hit, error) {
	if !b.Valid() || k < 1 || k > 100 {
		return nil, domain.ErrInvalid
	}
	return collect(s.Pool.Query(ctx, `SELECT `+hitColumns+`,0::float8,0::float8,0::bigint FROM search.documents WHERE tenant_id=$1 AND NOT deleted AND location IS NOT NULL AND `+boxClause(b.MinLon > b.MaxLon)+` ORDER BY id LIMIT $6`, tenant, b.MinLon, b.MinLat, b.MaxLon, b.MaxLat, k))
}
func (s *Store) Radius(ctx context.Context, tenant string, p domain.Point, km float64, k int) ([]api.Hit, error) {
	if !domain.ValidRadius(p, km, k) {
		return nil, domain.ErrInvalid
	}
	b := domain.RadiusBox(p, km)
	return collect(s.Pool.Query(ctx, `SELECT id,sku,warehouse,title,version,0::float8,km,0::bigint FROM (
SELECT `+hitColumns+`,2*6371.0088*asin(sqrt(least(1.0,greatest(0.0,
 power(sin(radians(latitude-$7)/2),2)+cos(radians($7))*cos(radians(latitude))*power(sin(radians(longitude-$6)/2),2))))) AS km
FROM search.documents WHERE tenant_id=$1 AND NOT deleted AND location IS NOT NULL AND `+boxClause(b.MinLon > b.MaxLon)+`) candidates WHERE km<=$8+1e-9 ORDER BY km,id LIMIT $9`, tenant, b.MinLon, b.MinLat, b.MaxLon, b.MaxLat, p.Lon, p.Lat, km, k))
}
