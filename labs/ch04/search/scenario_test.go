package search

import (
	"encoding/json"
	"testing"
)

func TestExecutableSearchScenario(t *testing.T) {
	r, e := RunScenarioAt(t.TempDir(), 64, 1)
	must(t, e)
	if !r.Passed || !r.ReopenVerified || !r.MutationVerified || len(r.Graphs) != 6 || len(r.Samples) != 288 || len(r.Spatial) != 3 {
		t.Fatal("incomplete report")
	}
	for _, s := range r.Samples {
		if s.Recall < 0 || s.Recall > 1 || s.DistanceEvaluations < 1 {
			t.Fatal(s)
		}
	}
	if _, e = json.Marshal(r); e != nil {
		t.Fatal(e)
	}
}
