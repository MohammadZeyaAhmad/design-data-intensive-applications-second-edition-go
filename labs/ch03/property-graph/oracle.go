package graph

import (
	"context"
	"sort"
)

// Oracle scans the complete edge relation rather than using adjacency indexes.
// Reach uses bounded relaxation; path enumeration is independent of the heap
// and DFS implementation under test. It is a correctness reference, not a peer
// performance baseline.
func Oracle(ctx context.Context, f Fixture, mode Mode, q Query) (Answer, error) {
	a := Empty()
	var e error
	q, e = Normalize(mode, q)
	if e != nil {
		return a, e
	}
	nodes := map[string]Node{}
	for _, n := range f.Nodes {
		if n.Tenant == q.Tenant {
			nodes[n.ID] = n
		}
	}
	if _, ok := nodes[q.Start]; !ok {
		return a, nil
	}
	type link struct {
		edge           Edge
		source, target string
	}
	links := []link{}
	for _, edge := range f.Edges {
		if edge.Tenant != q.Tenant || q.Kind != "all" && edge.Kind != q.Kind || edge.Properties["enabled"] != true || edge.Properties["capacity"].(int64) < q.MinCapacity {
			continue
		}
		from, to := edge.From, edge.To
		if q.Direction == "in" {
			from, to = to, from
		}
		links = append(links, link{edge, from, to})
	}
	if mode == Reach {
		dist := map[string]int{q.Start: 0}
		for i := 0; i < q.Hops; i++ {
			if e = ctx.Err(); e != nil {
				return Empty(), e
			}
			previous := map[string]int{}
			for id, d := range dist {
				previous[id] = d
			}
			for _, l := range links {
				d, ok := previous[l.source]
				if !ok {
					continue
				}
				old, exists := dist[l.target]
				if !exists || old > d+1 {
					dist[l.target] = d + 1
				}
			}
		}
		for id, d := range dist {
			n := nodes[id]
			a.Reached = append(a.Reached, Reached{id, n.Kind, n.Properties["name"].(string), d})
		}
		sort.Slice(a.Reached, func(i, j int) bool {
			if a.Reached[i].Hops == a.Reached[j].Hops {
				return a.Reached[i].ID < a.Reached[j].ID
			}
			return a.Reached[i].Hops < a.Reached[j].Hops
		})
		return a, nil
	}
	if _, ok := nodes[q.Target]; !ok {
		return a, nil
	}
	var walk func(string, []string, []string, int64) error
	walk = func(id string, vertices, edges []string, cost int64) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if id == q.Target {
			a.Paths = append(a.Paths, Path{append([]string{}, vertices...), append([]string{}, edges...), cost})
		}
		if len(edges) == q.Hops {
			return nil
		}
		for _, l := range links {
			if l.source != id {
				continue
			}
			repeated := false
			for _, v := range vertices {
				if v == l.target {
					repeated = true
				}
			}
			if repeated {
				continue
			}
			if e := walk(l.target, append(append([]string{}, vertices...), l.target), append(append([]string{}, edges...), l.edge.ID), cost+l.edge.Properties["cost"].(int64)); e != nil {
				return e
			}
		}
		return nil
	}
	if e = walk(q.Start, []string{q.Start}, []string{}, 0); e != nil {
		return Empty(), e
	}
	// Separate comparison logic avoids sharing the implementation's heap order.
	lex := func(x, y []string) int {
		for i := 0; i < len(x) && i < len(y); i++ {
			if x[i] != y[i] {
				if x[i] < y[i] {
					return -1
				}
				return 1
			}
		}
		if len(x) < len(y) {
			return -1
		}
		if len(x) > len(y) {
			return 1
		}
		return 0
	}
	sort.Slice(a.Paths, func(i, j int) bool {
		x, y := a.Paths[i], a.Paths[j]
		if mode == Cheapest && x.Cost != y.Cost {
			return x.Cost < y.Cost
		}
		if len(x.Edges) != len(y.Edges) {
			return len(x.Edges) < len(y.Edges)
		}
		if v := lex(x.Nodes, y.Nodes); v != 0 {
			return v < 0
		}
		return lex(x.Edges, y.Edges) < 0
	})
	if mode == Cheapest && len(a.Paths) > 1 {
		a.Paths = a.Paths[:1]
	}
	if mode == Paths && len(a.Paths) > MaxPaths {
		return Empty(), ErrBudget
	}
	return a, nil
}
func Clone(f Fixture) Fixture {
	out := Fixture{Nodes: make([]Node, len(f.Nodes)), Edges: make([]Edge, len(f.Edges))}
	for i, n := range f.Nodes {
		n.Properties = cloneProperties(n.Properties)
		out.Nodes[i] = n
	}
	for i, e := range f.Edges {
		e.Properties = cloneProperties(e.Properties)
		out.Edges[i] = e
	}
	return out
}
func Updated(f Fixture) Fixture {
	out := Clone(f)
	for i := range out.Edges {
		e := &out.Edges[i]
		if e.Tenant == "acme" && e.ID == "shortcut-02" {
			e.Properties["cost"] = int64(3)
		}
		if e.Tenant == "acme" && e.ID == "disabled-47" {
			e.Properties["enabled"] = true
			e.Properties["capacity"] = int64(91)
		}
	}
	for i := range out.Nodes {
		n := &out.Nodes[i]
		if n.Tenant == "acme" && n.ID == "wh-000" {
			n.Properties["name"] = "acme renamed hub"
		}
	}
	return out
}
func Deleted(f Fixture) Fixture {
	out := Clone(f)
	out.Nodes = nil
	out.Edges = nil
	for _, n := range f.Nodes {
		if n.Tenant == "acme" && n.ID == "wh-002" {
			continue
		}
		n.Properties = cloneProperties(n.Properties)
		out.Nodes = append(out.Nodes, n)
	}
	for _, e := range f.Edges {
		if e.Tenant == "acme" && (e.From == "wh-002" || e.To == "wh-002") {
			continue
		}
		e.Properties = cloneProperties(e.Properties)
		out.Edges = append(out.Edges, e)
	}
	return out
}
