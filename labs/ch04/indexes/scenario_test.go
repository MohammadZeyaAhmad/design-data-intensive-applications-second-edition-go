package indexes

import "testing"

func TestExecutableLayoutComparison(t *testing.T) {
	r, e := RunScenarioAt(t.TempDir(), 64, 1)
	must(t, e)
	if !r.Passed || len(r.Results) != 5 {
		t.Fatal("incomplete comparison")
	}
	for _, x := range r.Results {
		m := x.Runs[0]
		if m.Digest != r.Results[0].Runs[0].Digest || m.Cost.Returned == 0 {
			t.Fatal("different results", x)
		}
		if x.Layout != Memory && !m.ReopenVerified {
			t.Fatal("missing durable reopen", x)
		}
		if x.Layout == Memory && (m.DiskBytes != 0 || m.ReopenVerified) {
			t.Fatal("memory claimed persistence", x)
		}
		if x.Layout == Heap && m.Cost.RowsDecoded != uint64(m.Rows*m.Queries) {
			t.Fatal("heap omitted rows", m)
		}
	}
}
