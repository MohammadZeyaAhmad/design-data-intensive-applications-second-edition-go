// Package graph implements a bounded tenant-qualified directed property
// multigraph. SQL and Neo4j adapters execute the same explicit query contract.
package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaxNodes     = 512
	MaxEdges     = 2048
	MaxDegree    = 4
	MaxHops      = 6
	MaxReachHops = MaxNodes - 1
	MaxPaths     = 256
)

var ErrInvalid = errors.New("invalid graph fixture or query")
var ErrBudget = errors.New("graph path result exceeds bounded budget")
var ErrClosed = errors.New("graph backend is closed")
var name = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var propertyName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

type Properties map[string]any

func (p *Properties) UnmarshalJSON(b []byte) error {
	if len(b) > 32768 || !utf8.Valid(b) {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	var values map[string]any
	if decoder.Decode(&values) != nil || decoder.Decode(new(any)) != io.EOF || values == nil {
		return ErrInvalid
	}
	for k, v := range values {
		if n, ok := v.(json.Number); ok {
			x, e := n.Int64()
			if e != nil {
				return ErrInvalid
			}
			values[k] = x
		}
	}
	*p = Properties(values)
	return validateProperties(*p)
}
func validateProperties(p Properties) error {
	if p == nil || len(p) > 16 {
		return ErrInvalid
	}
	for k, v := range p {
		if !propertyName.MatchString(k) || k == "run" || k == "tenant" || k == "id" || k == "kind" {
			return ErrInvalid
		}
		switch x := v.(type) {
		case string:
			if len(x) > 256 || !utf8.ValidString(x) || strings.ContainsRune(x, 0) {
				return ErrInvalid
			}
		case int64:
			if x < -1e12 || x > 1e12 {
				return ErrInvalid
			}
		case bool:
		default:
			return ErrInvalid
		}
	}
	return nil
}
func cloneProperties(p Properties) Properties {
	out := Properties{}
	for k, v := range p {
		out[k] = v
	}
	return out
}

type Node struct {
	Tenant     string     `json:"tenant"`
	ID         string     `json:"id"`
	Kind       string     `json:"kind"`
	Properties Properties `json:"properties"`
}
type Edge struct {
	Tenant     string     `json:"tenant"`
	ID         string     `json:"id"`
	From       string     `json:"from"`
	To         string     `json:"to"`
	Kind       string     `json:"kind"`
	Properties Properties `json:"properties"`
}
type Fixture struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

func Key(tenant, id string) string { return tenant + "\x00" + id }
func Validate(f Fixture) error {
	if len(f.Nodes) == 0 || len(f.Nodes) > MaxNodes || len(f.Edges) > MaxEdges {
		return ErrInvalid
	}
	nodes := map[string]Node{}
	edges := map[string]bool{}
	incoming, outgoing := map[string]int{}, map[string]int{}
	for _, n := range f.Nodes {
		k := Key(n.Tenant, n.ID)
		if !name.MatchString(n.Tenant) || !name.MatchString(n.ID) || n.Kind != "warehouse" && n.Kind != "supplier" {
			return ErrInvalid
		}
		if _, ok := nodes[k]; ok {
			return ErrInvalid
		}
		if validateProperties(n.Properties) != nil {
			return ErrInvalid
		}
		label, ok := n.Properties["name"].(string)
		if !ok || label == "" {
			return ErrInvalid
		}
		nodes[k] = n
	}
	for _, e := range f.Edges {
		k := Key(e.Tenant, e.ID)
		a, okA := nodes[Key(e.Tenant, e.From)]
		b, okB := nodes[Key(e.Tenant, e.To)]
		if !name.MatchString(e.ID) || edges[k] || !okA || !okB || validateProperties(e.Properties) != nil {
			return ErrInvalid
		}
		if e.Kind == "route" {
			if a.Kind != "warehouse" || b.Kind != "warehouse" {
				return ErrInvalid
			}
		} else if e.Kind == "supplies" {
			if a.Kind != "supplier" || b.Kind != "warehouse" {
				return ErrInvalid
			}
		} else {
			return ErrInvalid
		}
		cost, ok := e.Properties["cost"].(int64)
		if !ok || cost < 1 || cost > 1e6 {
			return ErrInvalid
		}
		cap, ok := e.Properties["capacity"].(int64)
		if !ok || cap < 0 || cap > 1e6 {
			return ErrInvalid
		}
		if _, ok = e.Properties["enabled"].(bool); !ok {
			return ErrInvalid
		}
		edges[k] = true
		outgoing[Key(e.Tenant, e.From)]++
		incoming[Key(e.Tenant, e.To)]++
		if outgoing[Key(e.Tenant, e.From)] > MaxDegree || incoming[Key(e.Tenant, e.To)] > MaxDegree {
			return ErrInvalid
		}
	}
	body, e := json.Marshal(f)
	if e != nil || len(body) > 8<<20 {
		return ErrInvalid
	}
	return nil
}

type Mode string

const (
	Reach    Mode = "reach"
	Paths    Mode = "paths"
	Cheapest Mode = "cheapest"
)

type Query struct {
	Tenant      string `json:"tenant"`
	Start       string `json:"start"`
	Target      string `json:"target,omitempty"`
	Kind        string `json:"kind"`
	Direction   string `json:"direction"`
	Hops        int    `json:"max_hops"`
	MinCapacity int64  `json:"min_capacity"`
}

func Normalize(mode Mode, q Query) (Query, error) {
	if q.Kind == "" {
		q.Kind = "all"
	}
	if q.Direction == "" {
		q.Direction = "out"
	}
	bound := MaxHops
	if mode == Reach {
		bound = MaxReachHops
	}
	if mode != Reach && mode != Paths && mode != Cheapest || !name.MatchString(q.Tenant) || !name.MatchString(q.Start) || q.Kind != "all" && q.Kind != "route" && q.Kind != "supplies" || q.Direction != "out" && q.Direction != "in" || q.Hops < 0 || q.Hops > bound || q.MinCapacity < 0 || q.MinCapacity > 1e6 {
		return q, ErrInvalid
	}
	if mode == Reach {
		if q.Target != "" {
			return q, ErrInvalid
		}
	} else if !name.MatchString(q.Target) {
		return q, ErrInvalid
	}
	return q, nil
}

type Reached struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
	Hops int    `json:"hops"`
}
type Path struct {
	Nodes []string `json:"nodes"`
	Edges []string `json:"edges"`
	Cost  int64    `json:"cost"`
}
type Answer struct {
	Reached []Reached `json:"reached"`
	Paths   []Path    `json:"paths"`
}

func Empty() Answer { return Answer{Reached: []Reached{}, Paths: []Path{}} }
func compare(a, b []string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
func PathLess(a, b Path, weighted bool) bool {
	if weighted && a.Cost != b.Cost {
		return a.Cost < b.Cost
	}
	if len(a.Edges) != len(b.Edges) {
		return len(a.Edges) < len(b.Edges)
	}
	if c := compare(a.Nodes, b.Nodes); c != 0 {
		return c < 0
	}
	return compare(a.Edges, b.Edges) < 0
}
func Canonical(a Answer, mode Mode) Answer {
	if a.Reached == nil {
		a.Reached = []Reached{}
	}
	if a.Paths == nil {
		a.Paths = []Path{}
	}
	sort.Slice(a.Reached, func(i, j int) bool {
		if a.Reached[i].Hops != a.Reached[j].Hops {
			return a.Reached[i].Hops < a.Reached[j].Hops
		}
		return a.Reached[i].ID < a.Reached[j].ID
	})
	sort.Slice(a.Paths, func(i, j int) bool { return PathLess(a.Paths[i], a.Paths[j], mode == Cheapest) })
	return a
}

type Backend interface {
	Replace(context.Context, Fixture) error
	Query(context.Context, Mode, Query) (Answer, error)
	Close(context.Context) error
}
