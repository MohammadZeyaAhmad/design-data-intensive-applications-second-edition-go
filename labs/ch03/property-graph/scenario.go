package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"time"
)

type Sample struct {
	Backend     string `json:"backend"`
	Phase       string `json:"phase"`
	Case        string `json:"case"`
	Mode        Mode   `json:"mode"`
	Repetition  int    `json:"repetition"`
	ResultCount int    `json:"result_count"`
	Digest      string `json:"digest"`
	WallNanos   int64  `json:"wall_nanos"`
}
type Report struct {
	Passed      bool              `json:"passed"`
	Stats       Stats             `json:"initial_stats"`
	Go          string            `json:"go"`
	Repetitions int               `json:"repetitions"`
	Cases       []Case            `json:"cases"`
	Samples     []Sample          `json:"samples"`
	Results     map[string]Answer `json:"initial_answers"`
	Definitions []string          `json:"definitions"`
}

func Digest(a Answer) string {
	b, _ := json.Marshal(a)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func Count(a Answer) int { return len(a.Reached) + len(a.Paths) }
func CheckPhase(ctx context.Context, phase string, f Fixture, cases []Case, backends map[string]Backend, runs int) ([]Sample, error) {
	var samples []Sample
	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	sort.Strings(names)
	for caseIndex, c := range cases {
		want, e := Oracle(ctx, f, c.Mode, c.Query)
		if e != nil {
			return nil, e
		}
		for position := range names {
			name := names[(caseIndex+position)%len(names)]
			b := backends[name]
			warm, e := b.Query(ctx, c.Mode, c.Query)
			if e != nil || !reflect.DeepEqual(warm, want) {
				return nil, fmt.Errorf("%s %s %s warm comparison: %w", phase, name, c.Name, comparisonError(e))
			}
			for rep := 0; rep < runs; rep++ {
				start := time.Now()
				got, e := b.Query(ctx, c.Mode, c.Query)
				nanos := time.Since(start).Nanoseconds()
				if e != nil || !reflect.DeepEqual(got, want) {
					return nil, fmt.Errorf("%s %s %s: %w", phase, name, c.Name, comparisonError(e))
				}
				samples = append(samples, Sample{name, phase, c.Name, c.Mode, rep, Count(got), Digest(got), nanos})
			}
		}
	}
	return samples, nil
}
func comparisonError(e error) error {
	if e != nil {
		return e
	}
	return fmt.Errorf("graph answer differs from independent oracle")
}
func RunScenario(ctx context.Context, rows, runs int) (Report, error) {
	r := Report{Go: runtime.Version(), Repetitions: runs, Results: map[string]Answer{}}
	if runs < 1 || runs > 5 {
		return r, ErrInvalid
	}
	f, e := Synthetic(rows)
	if e != nil {
		return r, e
	}
	g, e := New(f)
	if e != nil {
		return r, e
	}
	defer g.Close(context.Background())
	r.Stats, _ = g.Stats()
	r.Cases = Cases(rows)
	for _, c := range r.Cases {
		a, e := Oracle(ctx, f, c.Mode, c.Query)
		if e != nil {
			return r, e
		}
		r.Results[c.Name] = a
	}
	for _, phase := range []string{"initial", "property-update", "vertex-delete"} {
		if phase == "property-update" {
			f = Updated(f)
		} else if phase == "vertex-delete" {
			f = Deleted(f)
		}
		if e = g.Replace(ctx, f); e != nil {
			return r, e
		}
		samples, e := CheckPhase(ctx, phase, f, r.Cases, map[string]Backend{"go-adjacency": g}, runs)
		if e != nil {
			return r, e
		}
		r.Samples = append(r.Samples, samples...)
	}
	r.Definitions = []string{"directed property multigraph, tenant-qualified identities, warehouse/supplier labels, native scalar properties and distinct parallel relationship IDs", "BFS reachability returns minimum hop distance and includes an existing start at zero; node-simple path enumeration distinguishes parallel edges", "bounded positive-cost Dijkstra selects cost, hops, node IDs then edge IDs; all-path order uses hops and identities", "both incoming/outgoing degree <=4; paths <=6 hops and <=256 results; reach <=511 hops covers the full <=512-vertex graph; overflow is an error", "independent edge-scan relaxation/enumeration defines expected answers across initial/property-update/vertex-delete phases", "caller values are cloned; queries and complete fixture replacement serialize through a local RWMutex; Go graph is volatile", "serialized fixture bytes are logical payload, not resident-memory/device-I/O measurement; backend execution order rotates by case; raw wall times describe warm process/OS state", "SQL/Cypher companion executes real recursive CTEs and Neo4j profiles over the same fixtures; no universal engine ranking"}
	r.Passed = true
	return r, nil
}
