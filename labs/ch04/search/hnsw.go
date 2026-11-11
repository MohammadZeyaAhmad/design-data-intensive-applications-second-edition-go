package search

import (
	"container/heap"
	"encoding/binary"
	"hash/fnv"
	"sort"
	"strconv"
)

type neighbor struct {
	id       string
	distance float64
}

func closer(a, b neighbor) bool {
	if a.distance != b.distance {
		return a.distance < b.distance
	}
	return a.id < b.id
}

type distanceHeap struct {
	items []neighbor
	worst bool
}

func (h distanceHeap) Len() int { return len(h.items) }
func (h distanceHeap) Less(i, j int) bool {
	if h.worst {
		return closer(h.items[j], h.items[i])
	}
	return closer(h.items[i], h.items[j])
}
func (h distanceHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *distanceHeap) Push(v any)   { h.items = append(h.items, v.(neighbor)) }
func (h *distanceHeap) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

type graphNode struct {
	id      string
	version uint64
	vector  []float64
	links   [][]string
}
type graph struct {
	nodes  map[string]*graphNode
	entry  string
	level  int
	config Config
	cached GraphStats
}
type GraphStats struct {
	Nodes              int `json:"nodes"`
	Edges              int `json:"directed_edges"`
	Levels             int `json:"levels"`
	VectorBytes        int `json:"float64_vector_bytes"`
	EdgeReferenceBytes int `json:"estimated_edge_reference_bytes"`
}
type VectorResult struct {
	Hits  []Hit      `json:"hits"`
	Work  Work       `json:"work"`
	Graph GraphStats `json:"graph"`
}

func (g *graph) measure(query []float64, id string, w *Work) neighbor {
	w.Distances++
	return neighbor{id, distance(query, g.nodes[id].vector, g.config.Metric)}
}
func (g *graph) greedy(query []float64, entry string, level int, w *Work) string {
	best := g.measure(query, entry, w)
	for {
		improved := false
		w.Nodes++
		node := g.nodes[best.id]
		if level >= len(node.links) {
			return best.id
		}
		for _, id := range node.links[level] {
			n := g.measure(query, id, w)
			if closer(n, best) {
				best = n
				improved = true
			}
		}
		if !improved {
			return best.id
		}
	}
}
func (g *graph) layer(query []float64, entry string, ef, level int, w *Work) []neighbor {
	first := g.measure(query, entry, w)
	candidates := &distanceHeap{items: []neighbor{first}}
	found := &distanceHeap{items: []neighbor{first}, worst: true}
	visited := map[string]bool{entry: true}
	for candidates.Len() > 0 {
		current := heap.Pop(candidates).(neighbor)
		if found.Len() >= ef && closer(found.items[0], current) {
			break
		}
		w.Nodes++
		for _, id := range g.nodes[current.id].links[level] {
			if visited[id] {
				continue
			}
			visited[id] = true
			n := g.measure(query, id, w)
			if found.Len() < ef || closer(n, found.items[0]) {
				heap.Push(candidates, n)
				heap.Push(found, n)
				if found.Len() > ef {
					heap.Pop(found)
				}
			}
		}
	}
	out := append([]neighbor(nil), found.items...)
	sort.Slice(out, func(i, j int) bool { return closer(out[i], out[j]) })
	return out
}
func (g *graph) selectNeighbors(query []float64, candidates []neighbor, limit int) []string {
	var chosen []string
	used := map[string]bool{}
	for _, c := range candidates {
		good := true
		for _, id := range chosen {
			if distance(g.nodes[c.id].vector, g.nodes[id].vector, g.config.Metric) < c.distance {
				good = false
				break
			}
		}
		if good {
			chosen = append(chosen, c.id)
			used[c.id] = true
			if len(chosen) == limit {
				return chosen
			}
		}
	}
	for _, c := range candidates {
		if !used[c.id] {
			chosen = append(chosen, c.id)
			if len(chosen) == limit {
				break
			}
		}
	}
	return chosen
}
func (g *graph) prune(id string, level, limit int) {
	node := g.nodes[id]
	if len(node.links[level]) <= limit {
		return
	}
	var candidates []neighbor
	for _, other := range node.links[level] {
		candidates = append(candidates, neighbor{other, distance(node.vector, g.nodes[other].vector, g.config.Metric)})
	}
	sort.Slice(candidates, func(i, j int) bool { return closer(candidates[i], candidates[j]) })
	node.links[level] = g.selectNeighbors(node.vector, candidates, limit)
}
func nodeLevel(id string, seed uint64) int {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seed)
	h.Write(b[:])
	h.Write([]byte(id))
	v := h.Sum64()
	level := 0
	for level < 8 && v&3 == 0 {
		level++
		v >>= 2
	}
	return level
}
func buildGraph(docs []Document, c Config) *graph {
	g := &graph{nodes: map[string]*graphNode{}, level: -1, config: c}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	for _, d := range docs {
		if d.Embedding == nil {
			continue
		}
		level := nodeLevel(d.ID, c.Seed)
		node := &graphNode{id: d.ID, version: d.Version, vector: append([]float64(nil), d.Embedding.Values...), links: make([][]string, level+1)}
		g.nodes[d.ID] = node
		if g.entry == "" {
			g.entry = d.ID
			g.level = level
			continue
		}
		ep := g.entry
		var work Work
		for l := g.level; l > level; l-- {
			ep = g.greedy(node.vector, ep, l, &work)
		}
		for l := min(g.level, level); l >= 0; l-- {
			candidates := g.layer(node.vector, ep, c.EfConstruction, l, &work)
			selected := g.selectNeighbors(node.vector, candidates, c.M)
			node.links[l] = selected
			for _, id := range selected {
				other := g.nodes[id]
				other.links[l] = append(other.links[l], d.ID)
				limit := c.M
				if l == 0 {
					limit *= 2
				}
				g.prune(id, l, limit)
			}
			if len(candidates) > 0 {
				ep = candidates[0].id
			}
		}
		if level > g.level {
			g.entry = d.ID
			g.level = level
		}
	}
	g.cached = g.calculateStats()
	return g
}
func (g *graph) stats() GraphStats { return g.cached }
func (g *graph) calculateStats() GraphStats {
	s := GraphStats{Nodes: len(g.nodes), Levels: max(0, g.level+1), VectorBytes: len(g.nodes) * g.config.Dimensions * 8}
	for _, n := range g.nodes {
		for _, links := range n.links {
			s.Edges += len(links)
		}
	}
	s.EdgeReferenceBytes = s.Edges * 2 * (strconv.IntSize / 8)
	return s
}
func (g *graph) query(v []float64, k, ef int) VectorResult {
	r := VectorResult{Graph: g.stats()}
	r.Work.Documents = len(g.nodes)
	if g.entry == "" {
		return r
	}
	entry := g.entry
	for l := g.level; l > 0; l-- {
		entry = g.greedy(v, entry, l, &r.Work)
	}
	neighbors := g.layer(v, entry, ef, 0, &r.Work)
	r.Work.Candidates = len(neighbors)
	if len(neighbors) > k {
		neighbors = neighbors[:k]
	}
	for _, n := range neighbors {
		r.Hits = append(r.Hits, Hit{ID: n.id, Version: g.nodes[n.id].version, Distance: n.distance})
	}
	return r
}
func exact(docs []Document, v []float64, k int, c Config) VectorResult {
	var r VectorResult
	for _, d := range docs {
		if d.Embedding == nil {
			continue
		}
		r.Work.Documents++
		r.Work.Distances++
		r.Hits = append(r.Hits, Hit{ID: d.ID, Version: d.Version, Distance: distance(v, d.Embedding.Values, c.Metric)})
	}
	sort.Slice(r.Hits, func(i, j int) bool {
		return closer(neighbor{r.Hits[i].ID, r.Hits[i].Distance}, neighbor{r.Hits[j].ID, r.Hits[j].Distance})
	})
	r.Work.Candidates = len(r.Hits)
	if len(r.Hits) > k {
		r.Hits = r.Hits[:k]
	}
	return r
}
func Recall(exact, approx []Hit) float64 {
	if len(exact) == 0 {
		if len(approx) == 0 {
			return 1
		}
		return 0
	}
	ids := map[string]bool{}
	for _, h := range exact {
		ids[h.ID] = true
	}
	seen := map[string]bool{}
	n := 0
	for _, h := range approx {
		if ids[h.ID] && !seen[h.ID] {
			n++
			seen[h.ID] = true
		}
	}
	return float64(n) / float64(len(exact))
}
