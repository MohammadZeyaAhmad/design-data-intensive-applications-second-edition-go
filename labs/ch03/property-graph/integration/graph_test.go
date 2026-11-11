//go:build graphintegration

package integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	graphnative "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/native"
	graphneo4j "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/neo4j"
	graphpostgres "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph/postgres"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func setup(t *testing.T) (context.Context, *graphnative.Environment, string, map[string]graph.Backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	env, e := graphnative.Open(ctx)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if e := env.Close(cleanup); e != nil {
			t.Error(e)
		}
	})
	id, e := graphnative.ID()
	if e != nil {
		t.Fatal(e)
	}
	run := "graph-" + id
	pg, e := graphpostgres.New(ctx, env.Pool, run)
	if e != nil {
		t.Fatal(e)
	}
	ng, e := graphneo4j.New(ctx, env.Driver, run)
	if e != nil {
		t.Fatal(e)
	}
	f, _ := graph.Synthetic(64)
	g, e := graph.New(f)
	if e != nil {
		t.Fatal(e)
	}
	stores := map[string]graph.Backend{"go": g, "postgres": pg, "neo4j": ng}
	for name, b := range stores {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if e := b.Close(cleanup); e != nil {
				t.Errorf("%s cleanup: %v", name, e)
			}
		})
		if e = b.Replace(ctx, f); e != nil {
			t.Fatal(e)
		}
	}
	return ctx, env, run, stores
}
func TestGraphServicesEquivalentQueriesPlansAndMutations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	r, e := graphnative.Run(ctx, 64, 1)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Samples) != 144 || len(r.Plans) != 15 || len(r.Publications) != 9 || r.Neo4j == "" || r.Postgres == "" {
		t.Fatal("incomplete native graph report")
	}
}
func TestGraphServicesTenantFencesAndExplicitBudget(t *testing.T) {
	ctx, env, run, stores := setup(t)
	f, _ := graph.Synthetic(64)
	q := graph.Query{Tenant: "acme", Start: "wh-000", Kind: "all", Hops: 6}
	want, e := graph.Oracle(ctx, f, graph.Reach, q)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = env.Pool.Exec(ctx, `INSERT INTO graph_lab.nodes(run,tenant,id,kind,properties) VALUES($1,'globex','bridge','warehouse','{"name":"foreign bridge"}')`, run); e != nil {
		t.Fatal(e)
	}
	_, e = env.Pool.Exec(ctx, `INSERT INTO graph_lab.edges(run,tenant,id,source,target,kind,properties) VALUES($1,'acme','rogue','wh-000','bridge','route','{"cost":1,"capacity":100,"enabled":true}')`, run)
	var constraint *pgconn.PgError
	if !errors.As(e, &constraint) || constraint.Code != "23503" {
		t.Fatal("SQL tenant-qualified foreign key did not reject bridge", e)
	}
	session := env.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(context.Background())
	result, e := session.Run(ctx, `MATCH(a:AtlasGraphVertex{run:$run,tenant:'acme',id:'wh-000'}),(z:AtlasGraphVertex{run:$run,tenant:'acme',id:'wh-007'}) CREATE(b:AtlasGraphVertex:Warehouse{run:$run,tenant:'globex',id:'bridge',kind:'warehouse',name:'foreign bridge'}) CREATE(a)-[:ROUTE{run:$run,tenant:'acme',id:'bridge-in',kind:'route',cost:1,capacity:100,enabled:true}]->(b)-[:ROUTE{run:$run,tenant:'acme',id:'bridge-out',kind:'route',cost:1,capacity:100,enabled:true}]->(z) CREATE(a)-[:ROUTE{run:$run,tenant:'globex',id:'wrong-tenant',kind:'route',cost:1,capacity:100,enabled:true}]->(z)`, map[string]any{"run": run})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = result.Consume(ctx); e != nil {
		t.Fatal(e)
	}
	for name, b := range stores {
		got, e := b.Query(ctx, graph.Reach, q)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s leaked tenant bridge: %+v (%v)", name, got, e)
		}
	}
	dense := graph.Fixture{}
	for i := 0; i < 7; i++ {
		dense.Nodes = append(dense.Nodes, graph.Node{Tenant: "acme", ID: fmt.Sprintf("n%d", i), Kind: "warehouse", Properties: graph.Properties{"name": fmt.Sprintf("Node %d", i)}})
	}
	for i := 0; i < 6; i++ {
		for j := 0; j < 3; j++ {
			dense.Edges = append(dense.Edges, graph.Edge{Tenant: "acme", ID: fmt.Sprintf("e%d-%d", i, j), From: fmt.Sprintf("n%d", i), To: fmt.Sprintf("n%d", i+1), Kind: "route", Properties: graph.Properties{"cost": int64(1), "capacity": int64(100), "enabled": true}})
		}
	}
	q = graph.Query{Tenant: "acme", Start: "n0", Target: "n6", Kind: "route", Hops: 6}
	for name, b := range stores {
		if e = b.Replace(ctx, dense); e != nil {
			t.Fatal(e)
		}
		if _, e = b.Query(ctx, graph.Paths, q); !errors.Is(e, graph.ErrBudget) {
			t.Fatalf("%s silently truncated path results: %v", name, e)
		}
		got, e := b.Query(ctx, graph.Cheapest, q)
		if e != nil || len(got.Paths) != 1 || got.Paths[0].Cost != 6 || !reflect.DeepEqual(got.Paths[0].Edges, []string{"e0-0", "e1-0", "e2-0", "e3-0", "e4-0", "e5-0"}) {
			t.Fatal(name, got, e)
		}
	}
}
func TestGraphServicesCompleteReplacementAndCancellation(t *testing.T) {
	ctx, _, _, stores := setup(t)
	f, _ := graph.Synthetic(64)
	updated := graph.Updated(f)
	q := graph.Cases(64)[0].Query
	old, _ := graph.Oracle(ctx, f, graph.Reach, q)
	fresh, _ := graph.Oracle(ctx, updated, graph.Reach, q)
	for name, b := range stores {
		var wg sync.WaitGroup
		fail := make(chan error, 4)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 4; j++ {
					if i == 0 {
						next := f
						if j%2 == 0 {
							next = updated
						}
						if e := b.Replace(ctx, next); e != nil {
							fail <- e
							return
						}
					} else {
						a, e := b.Query(ctx, graph.Reach, q)
						if e != nil || !reflect.DeepEqual(a, old) && !reflect.DeepEqual(a, fresh) {
							fail <- fmt.Errorf("mixed replacement from %s: %v", name, e)
							return
						}
					}
				}
			}(i)
		}
		wg.Wait()
		close(fail)
		for e := range fail {
			t.Error(e)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, e := b.Query(cancelled, graph.Reach, q); !errors.Is(e, context.Canceled) {
			t.Fatal(name, e)
		}
		invalid := q
		invalid.Hops = graph.MaxReachHops + 1
		if _, e := b.Query(ctx, graph.Reach, invalid); !errors.Is(e, graph.ErrInvalid) {
			t.Fatal(name, "accepted unbounded query", e)
		}
	}
}
