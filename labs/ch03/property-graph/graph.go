package graph

import (
	"container/heap"
	"context"
	"encoding/json"
	"sort"
	"sync"
)

type Stats struct {
	Nodes     int `json:"nodes"`
	Edges     int `json:"edges"`
	JSONBytes int `json:"serialized_fixture_bytes"`
}
type Graph struct {
	mu      sync.RWMutex
	nodes   map[string]Node
	out, in map[string][]Edge
	closed  bool
	stats   Stats
}

func New(f Fixture) (*Graph, error) {
	g := &Graph{}
	if e := g.Replace(context.Background(), f); e != nil {
		return nil, e
	}
	return g, nil
}
func (g *Graph) Replace(ctx context.Context, f Fixture) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := Validate(f); e != nil {
		return e
	}
	nodes := map[string]Node{}
	out, in := map[string][]Edge{}, map[string][]Edge{}
	for _, n := range f.Nodes {
		n.Properties = cloneProperties(n.Properties)
		nodes[Key(n.Tenant, n.ID)] = n
	}
	for _, e := range f.Edges {
		e.Properties = cloneProperties(e.Properties)
		out[Key(e.Tenant, e.From)] = append(out[Key(e.Tenant, e.From)], e)
		in[Key(e.Tenant, e.To)] = append(in[Key(e.Tenant, e.To)], e)
	}
	for _, index := range []map[string][]Edge{out, in} {
		for k := range index {
			sort.Slice(index[k], func(i, j int) bool { return index[k][i].ID < index[k][j].ID })
		}
	}
	body, _ := json.Marshal(f)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrClosed
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	g.nodes, g.out, g.in, g.stats = nodes, out, in, Stats{len(f.Nodes), len(f.Edges), len(body)}
	return nil
}
func (g *Graph) Stats() (Stats, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return Stats{}, ErrClosed
	}
	return g.stats, nil
}
func (g *Graph) Close(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	g.nodes, g.out, g.in = nil, nil, nil
	return nil
}
func eligible(e Edge, q Query) bool {
	return (q.Kind == "all" || e.Kind == q.Kind) && e.Properties["enabled"].(bool) && e.Properties["capacity"].(int64) >= q.MinCapacity
}
func (g *Graph) next(id string, q Query) []Edge {
	if q.Direction == "in" {
		return g.in[Key(q.Tenant, id)]
	}
	return g.out[Key(q.Tenant, id)]
}
func neighbor(e Edge, q Query) string {
	if q.Direction == "in" {
		return e.From
	}
	return e.To
}
func has(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
func extend(p Path, e Edge, q Query) Path {
	return Path{Nodes: append(append([]string{}, p.Nodes...), neighbor(e, q)), Edges: append(append([]string{}, p.Edges...), e.ID), Cost: p.Cost + e.Properties["cost"].(int64)}
}
func (g *Graph) Query(ctx context.Context, mode Mode, q Query) (Answer, error) {
	a := Empty()
	var e error
	q, e = Normalize(mode, q)
	if e != nil {
		return a, e
	}
	if e = ctx.Err(); e != nil {
		return a, e
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return a, ErrClosed
	}
	root, ok := g.nodes[Key(q.Tenant, q.Start)]
	if !ok {
		return a, nil
	}
	if mode == Reach {
		queue := []Reached{{root.ID, root.Kind, root.Properties["name"].(string), 0}}
		seen := map[string]bool{root.ID: true}
		for i := 0; i < len(queue); i++ {
			if e = ctx.Err(); e != nil {
				return Empty(), e
			}
			item := queue[i]
			if item.Hops == q.Hops {
				continue
			}
			for _, edge := range g.next(item.ID, q) {
				id := neighbor(edge, q)
				if !eligible(edge, q) || seen[id] {
					continue
				}
				seen[id] = true
				n := g.nodes[Key(q.Tenant, id)]
				queue = append(queue, Reached{n.ID, n.Kind, n.Properties["name"].(string), item.Hops + 1})
			}
		}
		a.Reached = queue
		return Canonical(a, mode), nil
	}
	if _, ok = g.nodes[Key(q.Tenant, q.Target)]; !ok {
		return a, nil
	}
	start := Path{Nodes: []string{q.Start}, Edges: []string{}}
	if mode == Cheapest {
		candidates := pathHeap{start}
		heap.Init(&candidates)
		for candidates.Len() > 0 {
			if e = ctx.Err(); e != nil {
				return Empty(), e
			}
			p := heap.Pop(&candidates).(Path)
			id := p.Nodes[len(p.Nodes)-1]
			if id == q.Target {
				a.Paths = append(a.Paths, p)
				return a, nil
			}
			if len(p.Edges) == q.Hops {
				continue
			}
			for _, edge := range g.next(id, q) {
				if eligible(edge, q) && !has(p.Nodes, neighbor(edge, q)) {
					heap.Push(&candidates, extend(p, edge, q))
				}
			}
		}
		return a, nil
	}
	var visit func(Path) error
	visit = func(p Path) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		id := p.Nodes[len(p.Nodes)-1]
		if id == q.Target {
			a.Paths = append(a.Paths, p)
			if len(a.Paths) > MaxPaths {
				return ErrBudget
			}
			return nil
		}
		if len(p.Edges) == q.Hops {
			return nil
		}
		for _, edge := range g.next(id, q) {
			if eligible(edge, q) && !has(p.Nodes, neighbor(edge, q)) {
				if e := visit(extend(p, edge, q)); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if e = visit(start); e != nil {
		return Empty(), e
	}
	return Canonical(a, mode), nil
}

type pathHeap []Path

func (h pathHeap) Len() int           { return len(h) }
func (h pathHeap) Less(i, j int) bool { return PathLess(h[i], h[j], true) }
func (h pathHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pathHeap) Push(x any)        { *h = append(*h, x.(Path)) }
func (h *pathHeap) Pop() any          { a := *h; x := a[len(a)-1]; *h = a[:len(a)-1]; return x }
