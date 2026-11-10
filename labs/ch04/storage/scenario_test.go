package storage

import "testing"

func TestWarehouseScenarioAgreesAcrossEmbeddedEngines(t *testing.T) {
	r, e := RunScenario(128, 2)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Results) != 4 {
		t.Fatal("missing alternatives")
	}
	for _, alternative := range r.Results {
		if len(alternative.Runs) != 2 {
			t.Fatal("missing repeated run")
		}
		for _, run := range alternative.Runs {
			if !run.ReopenVerified || run.DiskBytes == 0 || run.LiveBytes == 0 {
				t.Fatal("missing persistence evidence")
			}
		}
	}
}
