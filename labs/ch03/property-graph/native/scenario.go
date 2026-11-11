package graphnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	graphneo4j "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/neo4j"
	graphpostgres "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/postgres"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Plan struct {
	Backend string          `json:"backend"`
	Case    string          `json:"case"`
	Indexed bool            `json:"adjacency_indexes_added"`
	JSON    json.RawMessage `json:"actual_plan"`
}
type Publication struct {
	Backend string `json:"backend"`
	Phase   string `json:"phase"`
	Nanos   int64  `json:"wall_nanos"`
}
type Report struct {
	Passed       bool           `json:"passed"`
	Vertices     int            `json:"initial_vertices"`
	Edges        int            `json:"initial_edges"`
	Repetitions  int            `json:"repetitions"`
	Postgres     string         `json:"postgres_version"`
	Neo4j        string         `json:"neo4j_version"`
	Driver       string         `json:"neo4j_driver"`
	Cases        []graph.Case   `json:"cases"`
	Samples      []graph.Sample `json:"samples"`
	Plans        []Plan         `json:"plans"`
	Publications []Publication  `json:"fixture_publications"`
	Definitions  []string       `json:"definitions"`
}

func Run(ctx context.Context, rows, runs int) (report Report, resultErr error) {
	if runs < 1 || runs > 5 {
		return report, graph.ErrInvalid
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
		resultErr = errors.Join(resultErr, env.Close(cleanup))
	}()
	id, e := ID()
	if e != nil {
		return report, e
	}
	run := "graph-" + id
	pg, e := graphpostgres.New(ctx, env.Pool, run)
	if e != nil {
		return report, e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, pg.Close(cleanup))
	}()
	ng, e := graphneo4j.New(ctx, env.Driver, run)
	if e != nil {
		return report, e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, ng.Close(cleanup))
	}()
	goGraph, e := graph.New(fixture)
	if e != nil {
		return report, e
	}
	defer goGraph.Close(context.Background())
	report = Report{Vertices: len(fixture.Nodes), Edges: len(fixture.Edges), Repetitions: runs, Driver: "v5.28.4", Cases: graph.Cases(rows)}
	if e = env.Pool.QueryRow(ctx, "SHOW server_version").Scan(&report.Postgres); e != nil {
		return report, e
	}
	session := env.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(context.Background())
	r, e := session.Run(ctx, `CALL dbms.components() YIELD name,versions WHERE name='Neo4j Kernel' RETURN versions[0] AS version`, nil)
	if e != nil {
		return report, e
	}
	record, e := r.Single(ctx)
	if e != nil {
		return report, e
	}
	value, ok := record.Get("version")
	report.Neo4j, ok = value.(string)
	if !ok || report.Neo4j == "" {
		return report, graph.ErrInvalid
	}
	backends := map[string]graph.Backend{"go-adjacency": goGraph, "postgres-recursive": pg, "neo4j-cypher": ng}
	for _, phase := range []string{"initial", "property-update", "vertex-delete"} {
		if phase == "property-update" {
			fixture = graph.Updated(fixture)
		} else if phase == "vertex-delete" {
			fixture = graph.Deleted(fixture)
		}
		for _, name := range []string{"go-adjacency", "postgres-recursive", "neo4j-cypher"} {
			start := time.Now()
			if e = backends[name].Replace(ctx, fixture); e != nil {
				return report, e
			}
			report.Publications = append(report.Publications, Publication{name, phase, time.Since(start).Nanoseconds()})
		}
		if phase == "initial" {
			if e = pg.Analyze(ctx); e != nil {
				return report, e
			}
			for _, indexed := range []bool{false, true} {
				if indexed {
					if e = pg.EnsureIndexes(ctx); e != nil {
						return report, e
					}
					if e = pg.Analyze(ctx); e != nil {
						return report, e
					}
				}
				for _, i := range []int{0, 2, 5, 7, 15} {
					c := report.Cases[i]
					plan, e := pg.Profile(ctx, c.Mode, c.Query)
					if e != nil {
						return report, e
					}
					report.Plans = append(report.Plans, Plan{"postgres-recursive", c.Name, indexed, plan})
				}
			}
			for _, i := range []int{0, 2, 5, 7, 15} {
				c := report.Cases[i]
				plan, e := ng.Profile(ctx, c.Mode, c.Query)
				if e != nil {
					return report, e
				}
				if len(plan) == 0 || string(plan) == "null" {
					return report, fmt.Errorf("Neo4j profile absent")
				}
				report.Plans = append(report.Plans, Plan{"neo4j-cypher", c.Name, true, plan})
			}
		}
		samples, e := graph.CheckPhase(ctx, phase, fixture, report.Cases, backends, runs)
		if e != nil {
			return report, e
		}
		report.Samples = append(report.Samples, samples...)
	}
	// A property change must actually alter the relevant quality comparison.
	initial, _ := graph.Synthetic(rows)
	c := report.Cases[7]
	before, e := graph.Oracle(ctx, initial, c.Mode, c.Query)
	if e != nil {
		return report, e
	}
	after, e := graph.Oracle(ctx, graph.Updated(initial), c.Mode, c.Query)
	if e != nil {
		return report, e
	}
	if reflect.DeepEqual(before, after) {
		return report, fmt.Errorf("property-update fixture did not change cheapest route")
	}
	report.Definitions = []string{"identical bounded warehouse/supplier property multigraphs in Go adjacency maps, PostgreSQL node/edge tables and real Neo4j Community/Cypher", "all three backends match independent edge-scan relaxation/enumeration across initial/property-update/vertex-delete phases", "reach includes an existing zero-hop start and minimum hop distances; paths are node-simple and preserve parallel relationship identity", "cheapest path uses positive integer cost, then hops/node IDs/edge IDs; Go executes bounded Dijkstra, native queries enumerate the bounded simple-path relation", "both incoming/outgoing degree <=4 and six hops bound path enumeration; reach permits 511 hops, sufficient for full reachability in a <=512-vertex graph; >256 paths is an error", "PostgreSQL before/after phases refresh statistics; mandatory primary/foreign-key indexes remain; no forced planner switches", "Neo4j PROFILE trees contain actual operator/record/database-hit measurements; composite identity constraint supplies the anchor index", "each lab client serializes complete fixture replacement against its queries; this is not cross-client snapshot isolation or an online graph ingestion service", "all timings are warm client round trips/Go wall time; fixture publication is separate; PostgreSQL buffer hits and Neo4j DB hits are different metrics", "PostgreSQL uses an owned random database; Neo4j cleanup deletes only fresh run-scoped nodes/relationships; credentials remain in the child environment"}
	report.Passed = true
	return report, nil
}
