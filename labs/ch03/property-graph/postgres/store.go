package graphpostgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct {
	mu             sync.RWMutex
	pool           *pgxpool.Pool
	run            string
	closed, loaded bool
}

func New(ctx context.Context, pool *pgxpool.Pool, run string) (*Store, error) {
	tx, e := pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7392018503)`); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, schema); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO graph_lab.runs VALUES($1)`, run); e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return &Store{pool: pool, run: run}, nil
}
func (s *Store) Replace(ctx context.Context, f graph.Fixture) error {
	if e := graph.Validate(f); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return graph.ErrClosed
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `DELETE FROM graph_lab.nodes WHERE run=$1`, s.run); e != nil {
		return e
	}
	nodes := make([][]any, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		p, e := json.Marshal(n.Properties)
		if e != nil {
			return e
		}
		nodes = append(nodes, []any{s.run, n.Tenant, n.ID, n.Kind, p})
	}
	if _, e = tx.CopyFrom(ctx, pgx.Identifier{"graph_lab", "nodes"}, []string{"run", "tenant", "id", "kind", "properties"}, pgx.CopyFromRows(nodes)); e != nil {
		return e
	}
	edges := make([][]any, 0, len(f.Edges))
	for _, edge := range f.Edges {
		p, e := json.Marshal(edge.Properties)
		if e != nil {
			return e
		}
		edges = append(edges, []any{s.run, edge.Tenant, edge.ID, edge.From, edge.To, edge.Kind, p})
	}
	if _, e = tx.CopyFrom(ctx, pgx.Identifier{"graph_lab", "edges"}, []string{"run", "tenant", "id", "source", "target", "kind", "properties"}, pgx.CopyFromRows(edges)); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	s.loaded = true
	return nil
}
func sqlFor(mode graph.Mode, q graph.Query) string {
	from, to := "source", "target"
	if q.Direction == "in" {
		from, to = to, from
	}
	predicate := `e.run=$1 AND e.tenant=$2 AND e.enabled AND e.capacity >= $6 AND ($5='all' OR e.kind=$5)`
	if mode == graph.Reach {
		return fmt.Sprintf(`WITH RECURSIVE walk(id,depth) AS(SELECT id,0 FROM graph_lab.nodes WHERE run=$1 AND tenant=$2 AND id=$3 UNION SELECT e.%s,w.depth+1 FROM walk w JOIN graph_lab.edges e ON e.%s=w.id AND %s WHERE w.depth<$4) SELECT n.id,n.kind,n.properties->>'name',min(w.depth)::int FROM walk w JOIN graph_lab.nodes n ON n.run=$1 AND n.tenant=$2 AND n.id=w.id GROUP BY n.id,n.kind,n.properties ORDER BY min(w.depth),n.id COLLATE "C"`, to, from, predicate)
	}
	limit, order := "257", `cardinality(edge_ids),node_ids COLLATE "C",edge_ids COLLATE "C"`
	if mode == graph.Cheapest {
		limit = "1"
		order = `cost,cardinality(edge_ids),node_ids COLLATE "C",edge_ids COLLATE "C"`
	}
	return fmt.Sprintf(`WITH RECURSIVE walk(id,node_ids,edge_ids,cost) AS(SELECT id,ARRAY[id]::text[],ARRAY[]::text[],0::bigint FROM graph_lab.nodes WHERE run=$1 AND tenant=$2 AND id=$3 UNION ALL SELECT e.%s,w.node_ids||e.%s,w.edge_ids||e.id,w.cost+e.cost FROM walk w JOIN graph_lab.edges e ON e.%s=w.id AND %s WHERE cardinality(w.edge_ids)<$4 AND NOT(e.%s=ANY(w.node_ids))) SELECT node_ids,edge_ids,cost FROM walk WHERE id=$7 ORDER BY %s LIMIT %s`, to, to, from, predicate, to, order, limit)
}
func (s *Store) arguments(mode graph.Mode, q graph.Query) []any {
	a := []any{s.run, q.Tenant, q.Start, q.Hops, q.Kind, q.MinCapacity}
	if mode != graph.Reach {
		a = append(a, q.Target)
	}
	return a
}
func (s *Store) Query(ctx context.Context, mode graph.Mode, q graph.Query) (graph.Answer, error) {
	a := graph.Empty()
	var e error
	q, e = graph.Normalize(mode, q)
	if e != nil {
		return a, e
	}
	if e = ctx.Err(); e != nil {
		return a, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return a, graph.ErrClosed
	}
	if !s.loaded {
		return a, graph.ErrInvalid
	}
	rows, e := s.pool.Query(ctx, sqlFor(mode, q), s.arguments(mode, q)...)
	if e != nil {
		return a, e
	}
	defer rows.Close()
	for rows.Next() {
		if mode == graph.Reach {
			var r graph.Reached
			if e = rows.Scan(&r.ID, &r.Kind, &r.Name, &r.Hops); e != nil {
				return graph.Empty(), e
			}
			a.Reached = append(a.Reached, r)
		} else {
			var p graph.Path
			if e = rows.Scan(&p.Nodes, &p.Edges, &p.Cost); e != nil {
				return graph.Empty(), e
			}
			a.Paths = append(a.Paths, p)
		}
	}
	if e = rows.Err(); e != nil {
		return graph.Empty(), e
	}
	if mode == graph.Paths && len(a.Paths) > graph.MaxPaths {
		return graph.Empty(), graph.ErrBudget
	}
	return graph.Canonical(a, mode), nil
}
func (s *Store) Analyze(ctx context.Context) error {
	_, e := s.pool.Exec(ctx, `ANALYZE graph_lab.nodes;ANALYZE graph_lab.edges`)
	return e
}
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, e := s.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS graph_outgoing ON graph_lab.edges(run,tenant,source,kind);CREATE INDEX IF NOT EXISTS graph_incoming ON graph_lab.edges(run,tenant,target,kind)`)
	return e
}
func (s *Store) Profile(ctx context.Context, mode graph.Mode, q graph.Query) (json.RawMessage, error) {
	var e error
	q, e = graph.Normalize(mode, q)
	if e != nil {
		return nil, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, graph.ErrClosed
	}
	var b []byte
	e = s.pool.QueryRow(ctx, `EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) `+sqlFor(mode, q), s.arguments(mode, q)...).Scan(&b)
	return b, e
}
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if _, e := s.pool.Exec(ctx, `DELETE FROM graph_lab.runs WHERE id=$1`, s.run); e != nil {
		return e
	}
	s.closed = true
	return nil
}
