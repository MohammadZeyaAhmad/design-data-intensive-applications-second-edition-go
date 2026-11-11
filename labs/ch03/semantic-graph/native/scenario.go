package native

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	semantic "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph"
	sqlstore "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/sparql"
	"reflect"
	"time"
)

type Publication struct {
	Backend string `json:"backend"`
	Phase   string `json:"phase"`
	Nanos   int64  `json:"wall_nanos"`
}
type Plan struct {
	Case    string          `json:"case"`
	Backend string          `json:"backend"`
	Actual  json.RawMessage `json:"actual"`
}
type Report struct {
	semantic.Report
	PostgreSQL   string        `json:"postgresql_version"`
	RDFEngine    string        `json:"rdf_engine"`
	Publications []Publication `json:"publications"`
	Plans        []Plan        `json:"plans"`
}

func Run(ctx context.Context, rows, runs int) (report Report, err error) {
	if runs < 1 || runs > 5 {
		return report, semantic.ErrInvalid
	}
	fixture, e := graph.Synthetic(rows)
	if e != nil {
		return report, e
	}
	env, e := Open(ctx)
	if e != nil {
		return report, e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err = errors.Join(err, env.Close(cleanup))
	}()
	id, e := ID()
	if e != nil {
		return report, e
	}
	pg, e := sqlstore.New(ctx, env.Pool, id)
	if e != nil {
		return report, e
	}
	sg, e := sparql.New(env.RDF)
	if e != nil {
		return report, e
	}
	g, e := semantic.NewGo(fixture)
	if e != nil {
		return report, e
	}
	t, e := semantic.NewTriples(fixture)
	if e != nil {
		return report, e
	}
	d, e := semantic.NewDatalog(fixture)
	if e != nil {
		return report, e
	}
	names := []string{"go-graph", "go-rdf", "go-datalog", "postgres-recursive", "fuseki-sparql"}
	backends := map[string]semantic.Backend{"go-graph": g, "go-rdf": t, "go-datalog": d, "postgres-recursive": pg, "fuseki-sparql": sg}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, name := range names {
			err = errors.Join(err, backends[name].Close(cleanup))
		}
	}()
	report.Report = semantic.Report{Vertices: rows, Edges: len(fixture.Edges), Repetitions: runs, Cases: semantic.Cases(rows), Answers: map[string]semantic.Answer{}}
	for _, tenant := range []string{"acme", "globex"} {
		s, e := semantic.Triples(fixture, tenant)
		if e != nil {
			return report, e
		}
		report.Triples += len(s.Triples())
	}
	if e = env.Pool.QueryRow(ctx, "SHOW server_version").Scan(&report.PostgreSQL); e != nil {
		return report, e
	}
	report.RDFEngine = "Apache Jena Fuseki 5.6.0, TDB2; pinned runtime profile (see deploy/compose/rdf)"
	for _, c := range report.Cases {
		a, e := semantic.Oracle(ctx, fixture, c.Query)
		if e != nil {
			return report, e
		}
		report.Answers[c.Name] = a
	}
	for _, phase := range []string{"initial", "property-update", "vertex-delete"} {
		if phase == "property-update" {
			fixture = graph.Updated(fixture)
		} else if phase == "vertex-delete" {
			fixture = graph.Deleted(fixture)
		}
		for _, name := range names {
			start := time.Now()
			if e = backends[name].Replace(ctx, fixture); e != nil {
				return report, fmt.Errorf("publish %s: %w", name, e)
			}
			report.Publications = append(report.Publications, Publication{name, phase, time.Since(start).Nanoseconds()})
		}
		if _, e = env.Pool.Exec(ctx, "ANALYZE graph_lab.nodes; ANALYZE graph_lab.edges"); e != nil {
			return report, e
		}
		if phase == "initial" {
			for _, i := range []int{0, 4, 6, 8} {
				c := report.Cases[i]
				p, e := pg.Profile(ctx, c.Query)
				if e != nil {
					return report, e
				}
				report.Plans = append(report.Plans, Plan{c.Name, "postgres-recursive", p})
			}
		}
		samples, e := semantic.Check(ctx, phase, fixture, report.Cases, backends, runs)
		if e != nil {
			return report, e
		}
		report.Samples = append(report.Samples, samples...)
	}
	initial, _ := graph.Synthetic(rows)
	q := report.Cases[0].Query
	before, _ := semantic.Oracle(ctx, initial, q)
	after, _ := semantic.Oracle(ctx, graph.Updated(initial), q)
	deleted, _ := semantic.Oracle(ctx, graph.Deleted(graph.Updated(initial)), q)
	if reflect.DeepEqual(before, after) || reflect.DeepEqual(after, deleted) {
		return report, errors.New("fixture phases did not change canonical answers")
	}
	report.Definitions = []string{
		"five representations answer identical membership questions against an independent edge-scan oracle across three fixture phases",
		"SPARQL uses real authenticated Fuseki/TDB2: a per-query eligible-link named graph precedes DISTINCT property-path * or +; projection and cleanup are included in wall time",
		"each projected relationship requires the expected owner on the edge and both endpoints; named graph placement alone is insufficient tenant validation",
		"RDF terms, triple sets, SPARQL solution bags and DISTINCT membership are different semantics; parallel edge identity survives in source RDF",
		"Datalog is positive, function-free, range restricted, semi-naive and finite; capacity_ok is supplied explicitly by the host; deletion rebuilds facts",
		"PostgreSQL recursive UNION deduplicates vertex identities only and terminates on cycles without storing an unbounded hop count",
		"only PostgreSQL EXPLAIN ANALYZE plans are recorded; no fabricated Fuseki optimizer counters or cross-engine ranking",
		"PostgreSQL owns a fresh random database; RDF owns random source/scratch named graph URIs; cleanup preserves unrelated data",
	}
	report.Passed = true
	return report, nil
}
