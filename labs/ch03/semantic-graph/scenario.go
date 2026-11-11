package semantic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/datalog"
	"reflect"
	"sort"
	"time"
)

type Case struct {
	Name  string `json:"name"`
	Query Query  `json:"query"`
}

func Cases(rows int) []Case {
	isolated := fmt.Sprintf("wh-%03d", rows/2-4)
	return []Case{
		{"route-star", Query{Tenant: "acme", Start: "wh-000", Kind: "route", Reflexive: true}},
		{"other-tenant", Query{Tenant: "globex", Start: "wh-000", Kind: "route", Reflexive: true}},
		{"supplier-star", Query{Tenant: "acme", Start: "supplier-0", Kind: "all", Reflexive: true}},
		{"reverse-suppliers", Query{Tenant: "acme", Start: "wh-003", Kind: "all", Direction: "in", Reflexive: true}},
		{"capacity-95", Query{Tenant: "acme", Start: "wh-000", Kind: "route", MinCapacity: 95, Reflexive: true}},
		{"capacity-101", Query{Tenant: "acme", Start: "wh-000", Kind: "route", MinCapacity: 101, Reflexive: true}},
		{"cycle-plus", Query{Tenant: "acme", Start: "wh-000", Kind: "route"}},
		{"isolated-star", Query{Tenant: "acme", Start: isolated, Kind: "all", Reflexive: true}},
		{"isolated-plus", Query{Tenant: "acme", Start: isolated, Kind: "all"}},
		{"missing-star", Query{Tenant: "acme", Start: "missing", Kind: "all", Reflexive: true}},
		{"missing-tenant", Query{Tenant: "missing", Start: "wh-000", Kind: "all", Reflexive: true}},
		{"full-component", Query{Tenant: "acme", Start: "wh-007", Kind: "route", Reflexive: true}},
		{"supplier-no-route-plus", Query{Tenant: "acme", Start: "supplier-0", Kind: "route"}},
		{"supplier-plus", Query{Tenant: "globex", Start: "supplier-0", Kind: "all"}},
	}
}

type Sample struct {
	Backend    string         `json:"backend"`
	Phase      string         `json:"phase"`
	Case       string         `json:"case"`
	Repetition int            `json:"repetition"`
	Rows       int            `json:"rows"`
	Digest     string         `json:"digest"`
	Nanos      int64          `json:"wall_nanos"`
	Datalog    *datalog.Stats `json:"datalog,omitempty"`
}
type Report struct {
	Passed      bool              `json:"passed"`
	Vertices    int               `json:"initial_vertices"`
	Edges       int               `json:"initial_edges"`
	Triples     int               `json:"initial_triples"`
	Repetitions int               `json:"repetitions"`
	Cases       []Case            `json:"cases"`
	Samples     []Sample          `json:"samples"`
	Answers     map[string]Answer `json:"initial_answers"`
	Definitions []string          `json:"definitions"`
}

func Digest(a Answer) string {
	b, _ := json.Marshal(a)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func Check(ctx context.Context, phase string, f graph.Fixture, cases []Case, backends map[string]Backend, runs int) ([]Sample, error) {
	names := []string{}
	for name := range backends {
		names = append(names, name)
	}
	sort.Strings(names)
	out := []Sample{}
	for ci, c := range cases {
		want, e := Oracle(ctx, f, c.Query)
		if e != nil {
			return nil, e
		}
		for j := range names {
			name := names[(ci+j)%len(names)]
			b := backends[name]
			warm, e := b.Query(ctx, c.Query)
			if e != nil || !reflect.DeepEqual(warm, want) {
				return nil, fmt.Errorf("%s %s %s warm differs: %v", phase, name, c.Name, e)
			}
			for rep := 0; rep < runs; rep++ {
				start := time.Now()
				got, e := b.Query(ctx, c.Query)
				nanos := time.Since(start).Nanoseconds()
				if e != nil || !reflect.DeepEqual(got, want) {
					return nil, fmt.Errorf("%s %s %s differs: %v", phase, name, c.Name, e)
				}
				sample := Sample{name, phase, c.Name, rep, len(got), Digest(got), nanos, nil}
				if d, ok := b.(*Datalog); ok {
					stats := d.Stats()
					sample.Datalog = &stats
				}
				out = append(out, sample)
			}
		}
	}
	return out, nil
}
func Run(ctx context.Context, rows, runs int) (Report, error) {
	r := Report{Vertices: rows, Repetitions: runs, Cases: Cases(rows), Answers: map[string]Answer{}}
	if runs < 1 || runs > 5 {
		return r, ErrInvalid
	}
	f, e := graph.Synthetic(rows)
	if e != nil {
		return r, e
	}
	r.Edges = len(f.Edges)
	for _, tenant := range []string{"acme", "globex"} {
		s, e := Triples(f, tenant)
		if e != nil {
			return r, e
		}
		r.Triples += len(s.Triples())
	}
	g, e := NewGo(f)
	if e != nil {
		return r, e
	}
	defer g.Close(context.Background())
	t, e := NewTriples(f)
	if e != nil {
		return r, e
	}
	defer t.Close(context.Background())
	d, e := NewDatalog(f)
	if e != nil {
		return r, e
	}
	defer d.Close(context.Background())
	backends := map[string]Backend{"go-graph": g, "go-rdf": t, "go-datalog": d}
	for _, c := range r.Cases {
		a, e := Oracle(ctx, f, c.Query)
		if e != nil {
			return r, e
		}
		r.Answers[c.Name] = a
	}
	for _, phase := range []string{"initial", "property-update", "vertex-delete"} {
		if phase == "property-update" {
			f = graph.Updated(f)
		} else if phase == "vertex-delete" {
			f = graph.Deleted(f)
		}
		for _, b := range backends {
			if e = b.Replace(ctx, f); e != nil {
				return r, e
			}
		}
		samples, e := Check(ctx, phase, f, r.Cases, backends, runs)
		if e != nil {
			return r, e
		}
		r.Samples = append(r.Samples, samples...)
	}
	r.Definitions = []string{"the same validated warehouse/supplier graph is represented as RDF resource/literal triples and positive Datalog facts", "IRIs carry tenant-qualified identity; reified edge resources preserve parallel edge identities, while projected reach links use set semantics", "property-path star includes an existing zero-hop start; plus can return its start through a real cycle; missing vertices never appear through a phantom zero path", "Datalog is function-free, positive, range restricted and bounded; semi-naive delta joins compute the least fixed point; unsupported language features are errors", "capacity_ok is an explicit finite input relation supplied by the host, not hidden arithmetic or a built-in comparison operator", "every phase rebuilds from current facts; removing facts requires recomputation, not monotone accumulation of a previous result", "warm measurements include RDF eligibility projection or complete Datalog evaluation; different representation work is not a universal ranking"}
	r.Passed = true
	return r, nil
}
