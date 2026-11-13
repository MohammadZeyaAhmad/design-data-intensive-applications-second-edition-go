package postgres

import (
	"context"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
)

func (s *Store) BrandHistory(ctx context.Context, tenant, id string) ([]api.BrandSourceRecord, error) {
	rows, e := s.Pool.Query(ctx, `SELECT id,version,schema_version,body::text,metadata::text,recorded_at FROM catalog.content_history WHERE tenant_id=$1 AND kind='brand' AND id=$2 ORDER BY version LIMIT 257`, tenant, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []api.BrandSourceRecord{}
	for rows.Next() {
		record := api.BrandSourceRecord{Tenant: tenant}
		if e = rows.Scan(&record.ID, &record.Version, &record.SchemaVersion, &record.Body, &record.Metadata, &record.RecordedAt); e != nil {
			return nil, e
		}
		snapshot, e := api.UpcastBrandHistory(record.SchemaVersion, record.Body)
		if e != nil || snapshot.ID != record.ID || snapshot.Version != record.Version {
			return nil, api.ErrHistorySchema
		}
		record.SourceSHA256 = api.HistoryDigest(record.Body)
		record.MetadataSHA256 = api.HistoryDigest(record.Metadata)
		out = append(out, record)
		if len(out) > 256 {
			return nil, api.ErrHistorySchema
		}
	}
	return out, rows.Err()
}
