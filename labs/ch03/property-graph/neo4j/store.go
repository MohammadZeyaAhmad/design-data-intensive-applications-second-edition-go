package graphneo4j

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Store struct {
	mu             sync.RWMutex
	driver         neo4j.DriverWithContext
	run            string
	closed, loaded bool
}

func New(ctx context.Context, driver neo4j.DriverWithContext, run string) (*Store, error) {
	s := &Store{driver: driver, run: run}
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(context.Background())
	for _, sql := range []string{`CREATE CONSTRAINT atlas_graph_vertex_identity IF NOT EXISTS FOR(n:AtlasGraphVertex) REQUIRE(n.run,n.tenant,n.id) IS UNIQUE`, `CREATE CONSTRAINT atlas_graph_run_identity IF NOT EXISTS FOR(r:AtlasGraphRun) REQUIRE r.id IS UNIQUE`, `CALL db.awaitIndexes(30)`} {
		r, e := session.Run(ctx, sql, nil)
		if e != nil {
			return nil, e
		}
		if _, e = r.Consume(ctx); e != nil {
			return nil, e
		}
	}
	r, e := session.Run(ctx, `CREATE(:AtlasGraphRun{id:$run})`, map[string]any{"run": run})
	if e != nil {
		return nil, e
	}
	if _, e = r.Consume(ctx); e != nil {
		return nil, e
	}
	return s, nil
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
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j", AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, e := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		r, e := tx.Run(ctx, `MATCH(n:AtlasGraphVertex{run:$run}) DETACH DELETE n`, map[string]any{"run": s.run})
		if e != nil {
			return nil, e
		}
		if _, e = r.Consume(ctx); e != nil {
			return nil, e
		}
		for _, label := range []string{"warehouse", "supplier"} {
			rows := []any{}
			for _, n := range f.Nodes {
				if n.Kind == label {
					rows = append(rows, map[string]any{"tenant": n.Tenant, "id": n.ID, "kind": n.Kind, "properties": map[string]any(n.Properties)})
				}
			}
			nodeLabel := "Warehouse"
			if label == "supplier" {
				nodeLabel = "Supplier"
			}
			r, e = tx.Run(ctx, `UNWIND $rows AS row CREATE(n:AtlasGraphVertex:`+nodeLabel+`) SET n=row.properties,n.run=$run,n.tenant=row.tenant,n.id=row.id,n.kind=row.kind`, map[string]any{"run": s.run, "rows": rows})
			if e != nil {
				return nil, e
			}
			if _, e = r.Consume(ctx); e != nil {
				return nil, e
			}
		}
		for _, kind := range []string{"route", "supplies"} {
			rows := []any{}
			for _, edge := range f.Edges {
				if edge.Kind == kind {
					rows = append(rows, map[string]any{"tenant": edge.Tenant, "id": edge.ID, "from": edge.From, "to": edge.To, "kind": edge.Kind, "properties": map[string]any(edge.Properties)})
				}
			}
			edgeType := "ROUTE"
			if kind == "supplies" {
				edgeType = "SUPPLIES"
			}
			r, e = tx.Run(ctx, `UNWIND $rows AS row MATCH(a:AtlasGraphVertex{run:$run,tenant:row.tenant,id:row.from}),(b:AtlasGraphVertex{run:$run,tenant:row.tenant,id:row.to}) CREATE(a)-[r:`+edgeType+`]->(b) SET r=row.properties,r.run=$run,r.tenant=row.tenant,r.id=row.id,r.kind=row.kind`, map[string]any{"run": s.run, "rows": rows})
			if e != nil {
				return nil, e
			}
			if _, e = r.Consume(ctx); e != nil {
				return nil, e
			}
		}
		return nil, nil
	}, neo4j.WithTxTimeout(20*time.Second))
	if e == nil {
		s.loaded = true
	}
	return e
}
func cypher(mode graph.Mode, q graph.Query) string {
	types := "ROUTE|SUPPLIES"
	if q.Kind == "route" {
		types = "ROUTE"
	} else if q.Kind == "supplies" {
		types = "SUPPLIES"
	}
	arrow := fmt.Sprintf("-[:%s*0..%d]->", types, q.Hops)
	if q.Direction == "in" {
		arrow = fmt.Sprintf("<-[:%s*0..%d]-", types, q.Hops)
	}
	text := `MATCH(s:AtlasGraphVertex{run:$run,tenant:$tenant,id:$start}) MATCH p=(s)` + arrow + `(t:AtlasGraphVertex) WHERE all(n IN nodes(p) WHERE n.run=$run AND n.tenant=$tenant) AND all(r IN relationships(p) WHERE r.run=$run AND r.tenant=$tenant AND r.enabled=true AND r.capacity >= $capacity AND($kind='all' OR r.kind=$kind))`
	if mode == graph.Reach {
		return text + ` WITH t,min(length(p)) AS hops RETURN t.id AS id,t.kind AS kind,t.name AS name,hops ORDER BY hops,id`
	}
	text += ` AND t.id=$target AND all(n IN nodes(p) WHERE single(m IN nodes(p) WHERE m=n)) WITH [n IN nodes(p)|n.id] AS nodes,[r IN relationships(p)|r.id] AS edges,reduce(total=0,r IN relationships(p)|total+r.cost) AS cost `
	if mode == graph.Cheapest {
		return text + `RETURN nodes,edges,cost ORDER BY cost,size(edges),nodes,edges LIMIT 1`
	}
	return text + `RETURN nodes,edges,cost ORDER BY size(edges),nodes,edges LIMIT 257`
}
func (s *Store) parameters(q graph.Query) map[string]any {
	return map[string]any{"run": s.run, "tenant": q.Tenant, "start": q.Start, "target": q.Target, "capacity": q.MinCapacity, "kind": q.Kind}
}
func stringField(r *neo4j.Record, k string) (string, error) {
	v, ok := r.Get(k)
	s, typed := v.(string)
	if !ok || !typed {
		return "", errors.New("invalid Neo4j graph field")
	}
	return s, nil
}
func listField(r *neo4j.Record, k string) ([]string, error) {
	v, ok := r.Get(k)
	items, typed := v.([]any)
	if !ok || !typed {
		return nil, errors.New("invalid Neo4j graph list")
	}
	a := make([]string, 0, len(items))
	for _, v := range items {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("invalid Neo4j graph identity")
		}
		a = append(a, s)
	}
	return a, nil
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
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j", AccessMode: neo4j.AccessModeRead})
	defer session.Close(context.Background())
	value, e := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		r, e := tx.Run(ctx, cypher(mode, q), s.parameters(q))
		if e != nil {
			return nil, e
		}
		return r.Collect(ctx)
	}, neo4j.WithTxTimeout(10*time.Second))
	if e != nil {
		return a, e
	}
	records := value.([]*neo4j.Record)
	for _, r := range records {
		if mode == graph.Reach {
			id, e := stringField(r, "id")
			if e != nil {
				return graph.Empty(), e
			}
			kind, e := stringField(r, "kind")
			if e != nil {
				return graph.Empty(), e
			}
			name, e := stringField(r, "name")
			if e != nil {
				return graph.Empty(), e
			}
			v, ok := r.Get("hops")
			h, typed := v.(int64)
			if !ok || !typed || h < 0 || h > graph.MaxReachHops {
				return graph.Empty(), graph.ErrInvalid
			}
			a.Reached = append(a.Reached, graph.Reached{ID: id, Kind: kind, Name: name, Hops: int(h)})
		} else {
			nodes, e := listField(r, "nodes")
			if e != nil {
				return graph.Empty(), e
			}
			edges, e := listField(r, "edges")
			if e != nil {
				return graph.Empty(), e
			}
			v, ok := r.Get("cost")
			cost, typed := v.(int64)
			if !ok || !typed {
				return graph.Empty(), graph.ErrInvalid
			}
			a.Paths = append(a.Paths, graph.Path{Nodes: nodes, Edges: edges, Cost: cost})
		}
	}
	if mode == graph.Paths && len(a.Paths) > graph.MaxPaths {
		return graph.Empty(), graph.ErrBudget
	}
	return graph.Canonical(a, mode), nil
}
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(context.Background())
	for _, sql := range []string{`MATCH(n:AtlasGraphVertex{run:$run}) DETACH DELETE n`, `MATCH(r:AtlasGraphRun{id:$run}) DELETE r`} {
		r, e := session.Run(ctx, sql, map[string]any{"run": s.run})
		if e != nil {
			return e
		}
		if _, e = r.Consume(ctx); e != nil {
			return e
		}
	}
	s.closed = true
	return nil
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
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j", AccessMode: neo4j.AccessModeRead})
	defer session.Close(context.Background())
	r, e := session.Run(ctx, "PROFILE "+cypher(mode, q), s.parameters(q))
	if e != nil {
		return nil, e
	}
	if _, e = r.Collect(ctx); e != nil {
		return nil, e
	}
	summary, e := r.Consume(ctx)
	if e != nil {
		return nil, e
	}
	return json.Marshal(profile(summary.Profile()))
}

type Plan struct {
	Operator        string         `json:"operator"`
	Arguments       map[string]any `json:"arguments"`
	Rows            int64          `json:"rows"`
	DBHits          int64          `json:"db_hits"`
	PageCacheHits   int64          `json:"page_cache_hits"`
	PageCacheMisses int64          `json:"page_cache_misses"`
	Nanos           int64          `json:"operator_nanos"`
	Children        []Plan         `json:"children"`
}

func profile(p neo4j.ProfiledPlan) Plan {
	out := Plan{Operator: p.Operator(), Arguments: p.Arguments(), Rows: p.Records(), DBHits: p.DbHits(), PageCacheHits: p.PageCacheHits(), PageCacheMisses: p.PageCacheMisses(), Nanos: p.Time(), Children: []Plan{}}
	for _, child := range p.Children() {
		out.Children = append(out.Children, profile(child))
	}
	return out
}
