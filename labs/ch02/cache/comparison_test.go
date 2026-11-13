package cache

import "testing"

func TestWriteBehindAcknowledgmentAndOrderingCounterexamples(t *testing.T) {
	report, err := Run()
	if err != nil {
		t.Fatal(err)
	}
	if report.Acknowledged.Version != 2 || report.AfterCrash.Version != 1 || report.Unfenced.Version != 2 || report.Fenced.Version != 3 {
		t.Fatal(report)
	}
	// Explore all schedules of four unique writes, rather than only the reverse
	// example: a version fence must converge to the maximum in every permutation.
	var schedules func([]int64, []int64)
	schedules = func(prefix, remaining []int64) {
		if len(remaining) == 0 {
			origin := &Origin{}
			for _, v := range prefix {
				origin.Apply(Value{Version: v, Price: v * 100}, true)
			}
			if origin.Value.Version != 4 {
				t.Fatal("fence depends on delivery order", prefix, origin.Value)
			}
			return
		}
		for i, v := range remaining {
			next := append([]int64{}, remaining[:i]...)
			next = append(next, remaining[i+1:]...)
			schedules(append(append([]int64{}, prefix...), v), next)
		}
	}
	schedules(nil, []int64{1, 2, 3, 4})
}
