package columnstore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExecutableColumnScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wd, e := os.Getwd()
	must(t, e)
	parent, e := filepath.Rel(wd, t.TempDir())
	must(t, e)
	r, e := RunScenarioAt(ctx, parent, 64, 1)
	must(t, e)
	if !r.Passed || len(r.Runs) != 1 || len(r.Runs[0].Measurements) != 36 || len(r.Compression) != 4 || len(r.Programs) != 3 {
		t.Fatal("incomplete report")
	}
	for _, m := range r.Runs[0].Measurements {
		if m.StateVersion == 0 || m.Scan.WallNanos == 0 {
			t.Fatal("unmeasured query")
		}
		if m.Phase == "delta" && m.Scan.DeltaBytes == 0 {
			t.Fatal("delta byte accounting missing")
		}
	}
	if _, e = json.Marshal(r); e != nil {
		t.Fatal(e)
	}
}
