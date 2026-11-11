package semantic

import (
	"context"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/datalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"sync"
)

type GoGraph struct {
	mu      sync.RWMutex
	fixture graph.Fixture
	inner   *graph.Graph
	closed  bool
}

func NewGo(f graph.Fixture) (*GoGraph, error) {
	g, e := graph.New(f)
	if e != nil {
		return nil, e
	}
	return &GoGraph{fixture: graph.Clone(f), inner: g}, nil
}
func (g *GoGraph) Replace(ctx context.Context, f graph.Fixture) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return graph.ErrClosed
	}
	if e := g.inner.Replace(ctx, f); e != nil {
		return e
	}
	g.fixture = graph.Clone(f)
	return nil
}
func (g *GoGraph) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	return g.inner.Close(ctx)
}
func (g *GoGraph) Query(ctx context.Context, q Query) (Answer, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	var e error
	q, e = Normalize(q)
	if e != nil {
		return nil, e
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return nil, graph.ErrClosed
	}
	roots := []string{}
	if q.Reflexive {
		roots = append(roots, q.Start)
	} else {
		for _, edge := range g.fixture.Edges {
			if edge.Tenant != q.Tenant || q.Kind != "all" && q.Kind != edge.Kind || edge.Properties["enabled"] != true || edge.Properties["capacity"].(int64) < q.MinCapacity {
				continue
			}
			from, to := edge.From, edge.To
			if q.Direction == "in" {
				from, to = to, from
			}
			if from == q.Start {
				roots = append(roots, to)
			}
		}
	}
	seen := map[string]Vertex{}
	for _, root := range roots {
		answer, e := g.inner.Query(ctx, graph.Reach, graph.Query{Tenant: q.Tenant, Start: root, Kind: q.Kind, Direction: q.Direction, Hops: graph.MaxReachHops, MinCapacity: q.MinCapacity})
		if e != nil {
			return nil, e
		}
		for _, n := range answer.Reached {
			seen[n.ID] = Vertex{n.ID, n.Kind, n.Name}
		}
	}
	out := Answer{}
	for _, v := range seen {
		out = append(out, v)
	}
	return Canonical(out), nil
}

type Datalog struct {
	mu      sync.RWMutex
	fixture graph.Fixture
	closed  bool
	last    datalog.Stats
}

func NewDatalog(f graph.Fixture) (*Datalog, error) {
	d := &Datalog{}
	if e := d.Replace(context.Background(), f); e != nil {
		return nil, e
	}
	return d, nil
}
func (d *Datalog) Replace(ctx context.Context, f graph.Fixture) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := graph.Validate(f); e != nil {
		return e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return graph.ErrClosed
	}
	d.fixture = graph.Clone(f)
	return nil
}
func (d *Datalog) Close(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.fixture = graph.Fixture{}
	return nil
}
func (d *Datalog) Stats() datalog.Stats { d.mu.RLock(); defer d.mu.RUnlock(); return d.last }
func (d *Datalog) Query(ctx context.Context, q Query) (Answer, error) {
	var e error
	q, e = Normalize(q)
	if e != nil {
		return nil, e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, graph.ErrClosed
	}
	edgePattern := "edge(T,E,X,Y,K,C,true,W)"
	if q.Direction == "in" {
		edgePattern = "edge(T,E,Y,X,K,C,true,W)"
	}
	rules := `step(T,X,Y) :- ` + edgePattern + `, allowed_kind(K), capacity_ok(C), node(T,X,A,B), node(T,Y,D,F).
reachable(T,Y) :- start(T,X), step(T,X,Y).
reachable(T,Z) :- reachable(T,Y), step(T,Y,Z).`
	if q.Reflexive {
		rules += `reachable(T,X) :- start(T,X), node(T,X,K,N).`
	}
	program, e := datalog.Parse(rules)
	if e != nil {
		return nil, e
	}
	seeds := []datalog.Fact{}
	add := func(name string, values ...datalog.Value) {
		seeds = append(seeds, datalog.Fact{Name: name, Values: values})
	}
	for _, n := range d.fixture.Nodes {
		owner, _ := rdf.TenantURI(n.Tenant)
		uri, _ := rdf.EntityURI(n.Tenant, "node", n.ID)
		add("node", datalog.IRI(owner.Value), datalog.IRI(uri.Value), datalog.String(n.Kind), datalog.String(n.Properties["name"].(string)))
	}
	capacities := map[int64]bool{}
	for _, edge := range d.fixture.Edges {
		owner, _ := rdf.TenantURI(edge.Tenant)
		uri, _ := rdf.EntityURI(edge.Tenant, "edge", edge.ID)
		from, _ := rdf.EntityURI(edge.Tenant, "node", edge.From)
		to, _ := rdf.EntityURI(edge.Tenant, "node", edge.To)
		cap := edge.Properties["capacity"].(int64)
		add("edge", datalog.IRI(owner.Value), datalog.IRI(uri.Value), datalog.IRI(from.Value), datalog.IRI(to.Value), datalog.String(edge.Kind), datalog.Integer(cap), datalog.Boolean(edge.Properties["enabled"].(bool)), datalog.Integer(edge.Properties["cost"].(int64)))
		if cap >= q.MinCapacity {
			capacities[cap] = true
		}
	}
	for c := range capacities {
		add("capacity_ok", datalog.Integer(c))
	}
	for _, kind := range []string{"route", "supplies"} {
		if q.Kind == "all" || q.Kind == kind {
			add("allowed_kind", datalog.String(kind))
		}
	}
	owner, _ := rdf.TenantURI(q.Tenant)
	start, _ := rdf.EntityURI(q.Tenant, "node", q.Start)
	add("start", datalog.IRI(owner.Value), datalog.IRI(start.Value))
	result, e := datalog.Evaluate(ctx, program, seeds, datalog.DefaultLimits(), true)
	if e != nil {
		return nil, e
	}
	d.last = result.Stats
	facts, e := result.Facts("reachable")
	if e != nil {
		return nil, e
	}
	byURI := map[string]Vertex{}
	for _, n := range d.fixture.Nodes {
		if n.Tenant == q.Tenant {
			uri, _ := rdf.EntityURI(n.Tenant, "node", n.ID)
			byURI[uri.Value] = Vertex{n.ID, n.Kind, n.Properties["name"].(string)}
		}
	}
	out := Answer{}
	for _, fact := range facts {
		if fact.Values[0].Text != owner.Value {
			continue
		}
		v, ok := byURI[fact.Values[1].Text]
		if !ok {
			return nil, ErrInvalid
		}
		out = append(out, v)
	}
	return Canonical(out), nil
}
