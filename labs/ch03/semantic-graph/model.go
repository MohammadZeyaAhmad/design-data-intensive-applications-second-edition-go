package semantic

import (
	"context"
	"errors"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"sort"
)

var ErrInvalid = errors.New("invalid semantic query or result")

type Query struct {
	Tenant      string `json:"tenant"`
	Start       string `json:"start"`
	Kind        string `json:"kind"`
	Direction   string `json:"direction"`
	MinCapacity int64  `json:"min_capacity"`
	Reflexive   bool   `json:"reflexive"`
}

func Normalize(q Query) (Query, error) {
	base, e := graph.Normalize(graph.Reach, graph.Query{Tenant: q.Tenant, Start: q.Start, Kind: q.Kind, Direction: q.Direction, Hops: graph.MaxReachHops, MinCapacity: q.MinCapacity})
	if e != nil {
		return q, e
	}
	q.Kind, q.Direction = base.Kind, base.Direction
	return q, nil
}

type Vertex struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
}
type Answer []Vertex

func Canonical(a Answer) Answer {
	if a == nil {
		a = Answer{}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	return a
}

type Backend interface {
	Replace(context.Context, graph.Fixture) error
	Query(context.Context, Query) (Answer, error)
	Close(context.Context) error
}

func Class(kind string) rdf.Term {
	if kind == "warehouse" {
		return rdf.URI(rdf.Vocab + "Warehouse")
	}
	if kind == "supplier" {
		return rdf.URI(rdf.Vocab + "Supplier")
	}
	if kind == "route" {
		return rdf.URI(rdf.Vocab + "Route")
	}
	return rdf.URI(rdf.Vocab + "Supplies")
}
func Triples(f graph.Fixture, tenant string) (*rdf.Store, error) {
	if e := graph.Validate(f); e != nil {
		return nil, e
	}
	owner, e := rdf.TenantURI(tenant)
	if e != nil {
		return nil, e
	}
	out := rdf.New()
	add := func(s rdf.Term, p string, o rdf.Term) error {
		_, e := out.Add(rdf.Triple{Subject: s, Predicate: rdf.URI(p), Object: o})
		return e
	}
	properties := func(subject rdf.Term, p graph.Properties) error {
		for k, v := range p {
			var term rdf.Term
			switch x := v.(type) {
			case string:
				term = rdf.String(x)
			case int64:
				term = rdf.Integer(x)
			case bool:
				term = rdf.Boolean(x)
			default:
				return ErrInvalid
			}
			if e := add(subject, rdf.Vocab+"property/"+k, term); e != nil {
				return e
			}
		}
		return nil
	}
	for _, n := range f.Nodes {
		if n.Tenant != tenant {
			continue
		}
		subject, _ := rdf.EntityURI(tenant, "node", n.ID)
		for _, p := range []struct {
			predicate string
			object    rdf.Term
		}{{rdf.Type, Class(n.Kind)}, {rdf.Vocab + "owner", owner}, {rdf.Vocab + "id", rdf.String(n.ID)}, {rdf.Label, rdf.String(n.Properties["name"].(string))}} {
			if e = add(subject, p.predicate, p.object); e != nil {
				return nil, e
			}
		}
		if e = properties(subject, n.Properties); e != nil {
			return nil, e
		}
	}
	for _, edge := range f.Edges {
		if edge.Tenant != tenant {
			continue
		}
		subject, _ := rdf.EntityURI(tenant, "edge", edge.ID)
		from, _ := rdf.EntityURI(tenant, "node", edge.From)
		to, _ := rdf.EntityURI(tenant, "node", edge.To)
		for _, p := range []struct {
			predicate string
			object    rdf.Term
		}{{rdf.Type, Class(edge.Kind)}, {rdf.Vocab + "owner", owner}, {rdf.Vocab + "id", rdf.String(edge.ID)}, {rdf.Vocab + "from", from}, {rdf.Vocab + "to", to}, {rdf.Vocab + "cost", rdf.Integer(edge.Properties["cost"].(int64))}, {rdf.Vocab + "capacity", rdf.Integer(edge.Properties["capacity"].(int64))}, {rdf.Vocab + "enabled", rdf.Boolean(edge.Properties["enabled"].(bool))}} {
			if e = add(subject, p.predicate, p.object); e != nil {
				return nil, e
			}
		}
		if e = properties(subject, edge.Properties); e != nil {
			return nil, e
		}
	}
	return out, nil
}
func Oracle(ctx context.Context, f graph.Fixture, q Query) (Answer, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	var e error
	q, e = Normalize(q)
	if e != nil {
		return nil, e
	}
	nodes := map[string]graph.Node{}
	for _, n := range f.Nodes {
		if n.Tenant == q.Tenant {
			nodes[n.ID] = n
		}
	}
	if _, ok := nodes[q.Start]; !ok {
		return Answer{}, nil
	}
	seen := map[string]bool{}
	if q.Reflexive {
		seen[q.Start] = true
	}
	type pair struct{ from, to string }
	links := []pair{}
	for _, edge := range f.Edges {
		if edge.Tenant != q.Tenant || q.Kind != "all" && q.Kind != edge.Kind || edge.Properties["enabled"] != true || edge.Properties["capacity"].(int64) < q.MinCapacity {
			continue
		}
		a, b := edge.From, edge.To
		if q.Direction == "in" {
			a, b = b, a
		}
		links = append(links, pair{a, b})
		if a == q.Start {
			seen[b] = true
		}
	}
	for {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		changed := false
		for _, p := range links {
			if seen[p.from] && !seen[p.to] {
				seen[p.to] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	out := Answer{}
	for id := range seen {
		n := nodes[id]
		out = append(out, Vertex{id, n.Kind, n.Properties["name"].(string)})
	}
	return Canonical(out), nil
}
