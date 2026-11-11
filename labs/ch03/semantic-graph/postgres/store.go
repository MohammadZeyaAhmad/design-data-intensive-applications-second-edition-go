package semanticpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	graphpostgres "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/postgres"
	semantic "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
)

type Store struct {
	mu     sync.RWMutex
	pool   *pgxpool.Pool
	inner  *graphpostgres.Store
	run    string
	closed bool
}

func New(ctx context.Context, pool *pgxpool.Pool, run string) (*Store, error) {
	inner, e := graphpostgres.New(ctx, pool, run)
	if e != nil {
		return nil, e
	}
	if e = inner.EnsureIndexes(ctx); e != nil {
		return nil, e
	}
	return &Store{pool: pool, inner: inner, run: run}, nil
}
func (s *Store) Replace(ctx context.Context, f graph.Fixture) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return graph.ErrClosed
	}
	return s.inner.Replace(ctx, f)
}
func statement(q semantic.Query) string {
	from, to := "source", "target"
	if q.Direction == "in" {
		from, to = to, from
	}
	eligible := `e.run=$1 AND e.tenant=$2 AND e.enabled AND e.capacity >= $5 AND($4='all' OR e.kind=$4)`
	base := `SELECT id FROM graph_lab.nodes WHERE run=$1 AND tenant=$2 AND id=$3`
	if !q.Reflexive {
		base = fmt.Sprintf(`SELECT e.%s FROM graph_lab.edges e WHERE e.%s=$3 AND %s`, to, from, eligible)
	}
	return fmt.Sprintf(`WITH RECURSIVE reached(id) AS(%s UNION SELECT e.%s FROM reached r JOIN graph_lab.edges e ON e.%s=r.id AND %s) SELECT n.id,n.kind,n.properties->>'name' FROM reached r JOIN graph_lab.nodes n ON n.run=$1 AND n.tenant=$2 AND n.id=r.id ORDER BY n.id COLLATE "C"`, base, to, from, eligible)
}
func (s *Store) Query(ctx context.Context, q semantic.Query) (semantic.Answer, error) {
	var e error
	q, e = semantic.Normalize(q)
	if e != nil {
		return nil, e
	}
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, graph.ErrClosed
	}
	rows, e := s.pool.Query(ctx, statement(q), s.run, q.Tenant, q.Start, q.Kind, q.MinCapacity)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := semantic.Answer{}
	for rows.Next() {
		var v semantic.Vertex
		if e = rows.Scan(&v.ID, &v.Kind, &v.Name); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) Profile(ctx context.Context, q semantic.Query) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, graph.ErrClosed
	}
	var e error
	q, e = semantic.Normalize(q)
	if e != nil {
		return nil, e
	}
	var b []byte
	e = s.pool.QueryRow(ctx, `EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) `+statement(q), s.run, q.Tenant, q.Start, q.Kind, q.MinCapacity).Scan(&b)
	return b, e
}
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if e := s.inner.Close(ctx); e != nil {
		return e
	}
	s.closed = true
	return nil
}
