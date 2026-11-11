package postgres

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed vector.sql
var vectorSchema string

func EnsureVector(ctx context.Context, pool *pgxpool.Pool) error {
	tx, e := pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer cmdtx.Rollback(tx)
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7392018502)`); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS search.vector_schema(version integer PRIMARY KEY,checksum text NOT NULL)`); e != nil {
		return e
	}
	sum := sha256.Sum256([]byte(vectorSchema))
	checksum := hex.EncodeToString(sum[:])
	var saved string
	e = tx.QueryRow(ctx, `SELECT checksum FROM search.vector_schema WHERE version=1`).Scan(&saved)
	if e == nil {
		if saved != checksum {
			return errors.New("search vector schema checksum mismatch")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if _, e = tx.Exec(ctx, vectorSchema); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO search.vector_schema(version,checksum) VALUES(1,$1)`, checksum); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func CheckVector(ctx context.Context, pool *pgxpool.Pool) error {
	var version, checksum string
	if e := pool.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname='vector'`).Scan(&version); e != nil {
		return domain.ErrVectorDisabled
	}
	if version != "0.8.1" {
		return errors.New("search vector profile requires pgvector 0.8.1")
	}
	sum := sha256.Sum256([]byte(vectorSchema))
	if e := pool.QueryRow(ctx, `SELECT checksum FROM search.vector_schema WHERE version=1`).Scan(&checksum); e != nil || checksum != hex.EncodeToString(sum[:]) {
		return errors.New("search vector schema is not initialized or differs")
	}
	return nil
}
func VectorLiteral(values []float64) string {
	var parts []string
	for _, v := range values {
		parts = append(parts, strconv.FormatFloat(float64(float32(v)), 'g', -1, 32))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
func (s *Store) Embed(ctx context.Context, r commands.Request, d domain.Embedding) (domain.Embedding, bool, error) {
	if !s.VectorEnabled {
		return domain.Embedding{}, false, domain.ErrVectorDisabled
	}
	if !d.Valid() {
		return domain.Embedding{}, false, domain.ErrInvalid
	}
	d.Values = append([]float64(nil), d.Values...)
	return cmdtx.Execute(ctx, s.Pool, "search", r, func(tx pgx.Tx) (domain.Embedding, error) {
		var current int64
		var deleted bool
		e := tx.QueryRow(ctx, `SELECT version,deleted FROM search.documents WHERE tenant_id=$1 AND id=$2 FOR SHARE`, r.Tenant, d.ID).Scan(&current, &deleted)
		if errors.Is(e, pgx.ErrNoRows) || e == nil && deleted {
			return domain.Embedding{}, domain.ErrNotFound
		}
		if e != nil {
			return domain.Embedding{}, e
		}
		if current != d.DocumentVersion {
			return domain.Embedding{}, domain.ErrConflict
		}
		var previous int64
		e = tx.QueryRow(ctx, `SELECT version FROM search.embeddings WHERE tenant_id=$1 AND id=$2 AND model=$3 AND model_version=$4 FOR UPDATE`, r.Tenant, d.ID, d.Model, d.ModelVersion).Scan(&previous)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return domain.Embedding{}, e
		}
		if previous != d.Version {
			return domain.Embedding{}, domain.ErrConflict
		}
		d.Version++
		meta, e := provenance(ctx)
		if e != nil {
			return domain.Embedding{}, e
		}
		// Expected revision also fences concurrent first publications, which have
		// no row to lock yet. ON CONFLICT updates only the expected old revision.
		var published int64
		e = tx.QueryRow(ctx, `INSERT INTO search.embeddings(tenant_id,id,model,model_version,dimensions,document_version,version,embedding,metadata) VALUES($1,$2,$3,$4,8,$5,$6,$7::vector(8),$8)
ON CONFLICT(tenant_id,id,model,model_version) DO UPDATE SET document_version=excluded.document_version,version=excluded.version,embedding=excluded.embedding,metadata=excluded.metadata WHERE search.embeddings.version=$9 RETURNING version`, r.Tenant, d.ID, d.Model, d.ModelVersion, d.DocumentVersion, d.Version, VectorLiteral(d.Values), meta, d.Version-1).Scan(&published)
		if errors.Is(e, pgx.ErrNoRows) {
			return domain.Embedding{}, domain.ErrConflict
		}
		if e != nil {
			return domain.Embedding{}, e
		}
		for i, x := range d.Values {
			d.Values[i] = float64(float32(x))
		}
		body, _ := json.Marshal(d)
		if _, e = tx.Exec(ctx, `INSERT INTO search.embedding_history(tenant_id,id,model,model_version,version,body,metadata) VALUES($1,$2,$3,$4,$5,$6,$7)`, r.Tenant, d.ID, d.Model, d.ModelVersion, d.Version, body, meta); e != nil {
			return domain.Embedding{}, e
		}
		return d, nil
	})
}
func (s *Store) Vector(ctx context.Context, tenant, model, modelVersion string, values []float64, k int, metric string) ([]api.Hit, error) {
	if !s.VectorEnabled {
		return nil, domain.ErrVectorDisabled
	}
	if !domain.ValidVectorQuery(model, modelVersion, values, k, metric) {
		return nil, domain.ErrInvalid
	}
	operator := "<=>"
	if metric == "euclidean" {
		operator = "<->"
	}
	// +0 and the ID tie-break deliberately request an exact sorted baseline,
	// rather than allowing an ANN index to silently change the public query.
	return collect(s.Pool.Query(ctx, `SELECT d.id,d.sku,d.warehouse,d.title,d.version,0::float8,(e.embedding `+operator+` $4::vector(8))::float8,e.version FROM search.embeddings e JOIN search.documents d ON d.tenant_id=e.tenant_id AND d.id=e.id AND d.version=e.document_version WHERE e.tenant_id=$1 AND NOT d.deleted AND e.model=$2 AND e.model_version=$3 ORDER BY (e.embedding `+operator+` $4::vector(8))+0,d.id LIMIT $5`, tenant, model, modelVersion, VectorLiteral(values), k))
}
