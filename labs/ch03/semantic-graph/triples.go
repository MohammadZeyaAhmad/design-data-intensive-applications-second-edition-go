package semantic

import (
	"context"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"strconv"
	"sync"
)

type TripleGraph struct {
	mu     sync.RWMutex
	stores map[string]*rdf.Store
	closed bool
}

func NewTriples(f graph.Fixture) (*TripleGraph, error) {
	t := &TripleGraph{}
	if e := t.Replace(context.Background(), f); e != nil {
		return nil, e
	}
	return t, nil
}
func (t *TripleGraph) Replace(ctx context.Context, f graph.Fixture) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := graph.Validate(f); e != nil {
		return e
	}
	next := map[string]*rdf.Store{}
	for _, n := range f.Nodes {
		if next[n.Tenant] == nil {
			s, e := Triples(f, n.Tenant)
			if e != nil {
				return e
			}
			next[n.Tenant] = s
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return graph.ErrClosed
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	t.stores = next
	return nil
}
func (t *TripleGraph) Close(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.stores = nil
	return nil
}
func single(s *rdf.Store, subject rdf.Term, predicate string) (rdf.Term, error) {
	p := rdf.URI(predicate)
	rows, _ := s.Match(&subject, &p, nil)
	if len(rows) != 1 {
		return rdf.Term{}, ErrInvalid
	}
	return rows[0].Object, nil
}
func (t *TripleGraph) Query(ctx context.Context, q Query) (Answer, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	var e error
	q, e = Normalize(q)
	if e != nil {
		return nil, e
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return nil, graph.ErrClosed
	}
	s := t.stores[q.Tenant]
	if s == nil {
		return Answer{}, nil
	}
	owner, _ := rdf.TenantURI(q.Tenant)
	typeTerm := rdf.URI(rdf.Type)
	vertices := map[rdf.Term]Vertex{}
	for _, kind := range []string{"warehouse", "supplier"} {
		class := Class(kind)
		rows, _ := s.Match(nil, &typeTerm, &class)
		for _, r := range rows {
			own, e := single(s, r.Subject, rdf.Vocab+"owner")
			if e != nil || own != owner {
				continue
			}
			id, e := rdf.NodeID(r.Subject, q.Tenant)
			if e != nil {
				return nil, e
			}
			name, e := single(s, r.Subject, rdf.Label)
			if e != nil || name.Kind != rdf.Literal || name.Datatype != rdf.XSD+"string" {
				return nil, ErrInvalid
			}
			vertices[r.Subject] = Vertex{id, kind, name.Value}
		}
	}
	start, _ := rdf.EntityURI(q.Tenant, "node", q.Start)
	if _, ok := vertices[start]; !ok {
		return Answer{}, nil
	}
	links := map[rdf.Term]map[rdf.Term]bool{}
	for _, kind := range []string{"route", "supplies"} {
		if q.Kind != "all" && q.Kind != kind {
			continue
		}
		class := Class(kind)
		edges, _ := s.Match(nil, &typeTerm, &class)
		for _, r := range edges {
			if e = ctx.Err(); e != nil {
				return nil, e
			}
			own, e := single(s, r.Subject, rdf.Vocab+"owner")
			if e != nil || own != owner {
				continue
			}
			enabled, e := single(s, r.Subject, rdf.Vocab+"enabled")
			if e != nil || enabled != rdf.Boolean(true) {
				continue
			}
			cap, e := single(s, r.Subject, rdf.Vocab+"capacity")
			if e != nil || cap.Datatype != rdf.XSD+"integer" {
				return nil, ErrInvalid
			}
			capacity, e := strconv.ParseInt(cap.Value, 10, 64)
			if e != nil {
				return nil, e
			}
			if capacity < q.MinCapacity {
				continue
			}
			from, e := single(s, r.Subject, rdf.Vocab+"from")
			if e != nil {
				return nil, e
			}
			to, e := single(s, r.Subject, rdf.Vocab+"to")
			if e != nil {
				return nil, e
			}
			if _, ok := vertices[from]; !ok {
				continue
			}
			if _, ok := vertices[to]; !ok {
				continue
			}
			if q.Direction == "in" {
				from, to = to, from
			}
			if links[from] == nil {
				links[from] = map[rdf.Term]bool{}
			}
			links[from][to] = true
		}
	}
	seen := map[rdf.Term]bool{}
	queue := []rdf.Term{}
	if q.Reflexive {
		seen[start] = true
		queue = append(queue, start)
	}
	for v := range links[start] {
		if !seen[v] {
			seen[v] = true
			queue = append(queue, v)
		}
	}
	for i := 0; i < len(queue); i++ {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		for v := range links[queue[i]] {
			if !seen[v] {
				seen[v] = true
				queue = append(queue, v)
			}
		}
	}
	out := Answer{}
	for v := range seen {
		out = append(out, vertices[v])
	}
	return Canonical(out), nil
}
