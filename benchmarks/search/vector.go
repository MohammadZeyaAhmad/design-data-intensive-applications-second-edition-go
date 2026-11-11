// Package searchbench measures real pgvector retrieval against an exact sorted
// baseline. Fixtures are scoped to fresh tenant IDs in an isolated test database.
package searchbench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	searchdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	searchembed "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/embed"
	searchpublish "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/publish"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Sample struct {
	Metric      string  `json:"metric"`
	EfSearch    int     `json:"ef_search"`
	Query       int     `json:"query"`
	Repetition  int     `json:"repetition"`
	Recall      float64 `json:"recall_at_k"`
	ExactNanos  int64   `json:"exact_nanos"`
	ApproxNanos int64   `json:"ann_nanos"`
	Rows        int     `json:"rows"`
}
type Plan struct {
	Metric   string          `json:"metric"`
	EfSearch int             `json:"ef_search"`
	JSON     json.RawMessage `json:"explain_analyze_buffers"`
}
type Report struct {
	Passed      bool     `json:"passed"`
	Rows        int      `json:"fixture_rows"`
	K           int      `json:"k"`
	Queries     int      `json:"queries"`
	Repetitions int      `json:"repetitions"`
	Postgres    string   `json:"postgres_version"`
	Vector      string   `json:"pgvector_version"`
	Samples     []Sample `json:"samples"`
	Plans       []Plan   `json:"plans"`
	Definitions []string `json:"definitions"`
}

func queryVector(rng *rand.Rand) []float64 {
	v := make([]float64, 8)
	for i := range v {
		v[i] = float64(float32(rng.Float64()*2 - 1))
	}
	return v
}
func recall(want, got []api.Hit) float64 {
	if len(want) == 0 {
		if len(got) == 0 {
			return 1
		}
		return 0
	}
	seen := map[string]bool{}
	for _, h := range want {
		seen[h.ID] = true
	}
	count := 0
	used := map[string]bool{}
	for _, h := range got {
		if seen[h.ID] && !used[h.ID] {
			count++
			used[h.ID] = true
		}
	}
	return float64(count) / float64(len(want))
}
func ann(ctx context.Context, tx pgx.Tx, tenant string, v []float64, k int, metric string) ([]api.Hit, error) {
	op := "<=>"
	if metric == "euclidean" {
		op = "<->"
	}
	rows, e := tx.Query(ctx, annSQL(op), tenant, searchdb.VectorLiteral(v), k)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []api.Hit
	for rows.Next() {
		var h api.Hit
		if e = rows.Scan(&h.ID, &h.Distance); e != nil {
			return nil, e
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
func annSQL(op string) string {
	return `SELECT e.id,(e.embedding ` + op + ` $2::vector(8))::float8 FROM search.embeddings e JOIN search.documents d ON d.tenant_id=e.tenant_id AND d.id=e.id AND d.version=e.document_version WHERE e.tenant_id=$1 AND e.model='atlas-synthetic' AND e.model_version='1' AND NOT d.deleted ORDER BY e.embedding ` + op + ` $2::vector(8) LIMIT $3`
}
func Run(ctx context.Context, pool *pgxpool.Pool, rows, queries, repetitions int) (Report, error) {
	var report Report
	if rows < 64 || rows > 4096 || queries < 1 || queries > 64 || repetitions < 1 || repetitions > 5 {
		return report, domain.ErrInvalid
	}
	if e := searchdb.EnsureVector(ctx, pool); e != nil {
		return report, e
	}
	if e := searchdb.CheckVector(ctx, pool); e != nil {
		return report, e
	}
	nonce, e := identity.NewID()
	if e != nil {
		return report, e
	}
	tenant := "vector-" + strings.ReplaceAll(nonce, "-", "")
	other := "other-" + strings.ReplaceAll(nonce, "-", "")
	store := &searchdb.Store{Pool: pool, VectorEnabled: true}
	rng := rand.New(rand.NewSource(405))
	report = Report{Rows: rows, K: 8, Queries: queries, Repetitions: repetitions}
	if e = pool.QueryRow(ctx, "SHOW server_version").Scan(&report.Postgres); e != nil {
		return report, e
	}
	if e = pool.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname='vector'`).Scan(&report.Vector); e != nil {
		return report, e
	}
	for i := 0; i < rows; i++ {
		owner := tenant
		if i%2 != 0 {
			owner = other
		}
		id := fmt.Sprintf("sku-%05d", i)
		d, _, e := (searchpublish.Handler{Store: store}).Handle(ctx, owner, "doc-"+id, domain.Document{ID: id, SKU: id, Warehouse: "east", Title: "Synthetic search fixture", Body: "Precomputed numeric coordinates"})
		if e != nil {
			return report, e
		}
		_, _, e = (searchembed.Handler{Store: store}).Handle(ctx, owner, "embedding-"+id, domain.Embedding{ID: id, DocumentVersion: d.Version, Model: "atlas-synthetic", ModelVersion: "1", Dimensions: 8, Values: queryVector(rng)})
		if e != nil {
			return report, e
		}
	}
	if _, e = pool.Exec(ctx, `ANALYZE search.embeddings; ANALYZE search.documents`); e != nil {
		return report, e
	}
	var vectors [][]float64
	for i := 0; i < queries; i++ {
		vectors = append(vectors, queryVector(rng))
	}
	for _, metric := range []string{"cosine", "euclidean"} {
		for _, ef := range []int{8, 32, 128} {
			tx, e := pool.Begin(ctx)
			if e != nil {
				return report, e
			}
			func() {
				defer tx.Rollback(context.Background())
				// Diagnostic engine selection is explicit. These switches are LOCAL,
				// and the report does not label forced plans as normal planner choices.
				_, e = tx.Exec(ctx, fmt.Sprintf(`SET LOCAL enable_seqscan=off; SET LOCAL enable_bitmapscan=off; SET LOCAL enable_sort=off; SET LOCAL hnsw.ef_search=%d; SET LOCAL hnsw.iterative_scan='strict_order'`, ef))
				if e != nil {
					return
				}
				op, index := "<=>", "search_vectors_cosine_hnsw"
				if metric == "euclidean" {
					op, index = "<->", "search_vectors_l2_hnsw"
				}
				var plan []byte
				e = tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+annSQL(op), tenant, searchdb.VectorLiteral(vectors[0]), report.K).Scan(&plan)
				if e != nil {
					return
				}
				if !strings.Contains(string(plan), `"Index Name": "`+index+`"`) {
					e = errors.New("diagnostic vector plan did not use expected HNSW index")
					return
				}
				report.Plans = append(report.Plans, Plan{metric, ef, plan})
				for qi, v := range vectors {
					want, err := store.Vector(ctx, tenant, "atlas-synthetic", "1", v, report.K, metric)
					if err != nil {
						e = err
						return
					}
					if _, err = ann(ctx, tx, tenant, v, report.K, metric); err != nil {
						e = err
						return
					}
					for rep := 0; rep < repetitions; rep++ {
						start := time.Now()
						want, err = store.Vector(ctx, tenant, "atlas-synthetic", "1", v, report.K, metric)
						exactTime := time.Since(start).Nanoseconds()
						if err != nil {
							e = err
							return
						}
						start = time.Now()
						got, err := ann(ctx, tx, tenant, v, report.K, metric)
						annTime := time.Since(start).Nanoseconds()
						if err != nil {
							e = err
							return
						}
						for _, h := range got {
							var active bool
							if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM search.documents WHERE tenant_id=$1 AND id=$2 AND NOT deleted)`, tenant, h.ID).Scan(&active); err != nil {
								e = err
								return
							}
							if !active || math.IsNaN(h.Distance) || math.IsInf(h.Distance, 0) {
								e = errors.New("ANN returned ineligible identity/distance")
								return
							}
						}
						report.Samples = append(report.Samples, Sample{metric, ef, qi, rep, recall(want, got), exactTime, annTime, len(got)})
					}
				}
			}()
			if e != nil {
				return report, e
			}
		}
	}
	report.Definitions = []string{"pgvector 0.8.1, vector(8) float32; deterministic synthetic numeric vectors, not language-model inference", "tenant/model/document-version filters apply to both exact and ANN queries; mixed-tenant graph traversal can still affect recall", "public exact baseline uses distance+0 and deterministic ID ordering to prevent implicit ANN selection", "ANN diagnostic sets LOCAL enable_seqscan/bitmapscan/sort off to select HNSW; all six actual JSON plans are retained; these are deliberately forced plans", "HNSW ef_search 8/32/128 with strict-order iterative scans; all raw recall/latency samples retained; no universal performance ranking", "native Go tests separately verify embedding revision races, stale-document exclusion, deletes, metadata and HTTP boundaries"}
	report.Passed = true
	return report, nil
}
