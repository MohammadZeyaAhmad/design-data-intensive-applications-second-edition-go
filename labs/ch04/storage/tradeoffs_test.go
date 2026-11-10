package storage

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/btree"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

func TestControlledTradeoffsHaveSameStateAndScopedMetrics(t *testing.T) {
	r, e := RunTradeoffsAt(t.TempDir(), 64, 2)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Results) != 16 || len(r.Summary) != 8 {
		t.Fatal("missing engine/order/repetition", r)
	}
	for _, x := range r.Results {
		if x.Digest != r.Results[0].Digest || x.LogicalBytes == 0 || x.LiveBytes == 0 || x.DiskBytes == 0 {
			t.Fatal("invalid state or denominators", x)
		}
		if x.Engine == "bbolt" {
			if x.WriteIO != nil || x.WriteAmplification != nil {
				t.Fatal("invented bbolt instrumentation")
			}
		} else {
			if x.WriteIO == nil || x.WriteIO.WriteBytes < x.LogicalBytes || x.WriteIO.Syncs == 0 {
				t.Fatal("unmeasured synchronous writes", x)
			}
		}
		if len(x.Warm) != 3 || len(x.Reopened) != 3 {
			t.Fatal("missing read phases")
		}
		for _, p := range append(x.Warm, x.Reopened...) {
			if p.Queries == 0 {
				t.Fatal("empty measurement")
			}
			if p.Kind == "point-miss" && (p.UsefulBytes != 0 || p.Amplification != nil) {
				t.Fatal("miss amplification has invalid denominator")
			}
			if p.IO != nil && p.IO.WriteBytes != 0 {
				t.Fatal("read phase includes maintenance writes")
			}
		}
	}
	if _, e = json.Marshal(r); e != nil {
		t.Fatal("report has non-finite numbers", e)
	}
	if _, e = RunTradeoffsAt("", 1, 1); !errors.Is(e, kv.ErrInvalid) {
		t.Fatal(e)
	}
}
func TestDistributionRecordsVariance(t *testing.T) {
	d := distribution([]int64{10, 20, 30})
	if d.Mean != 20 || d.Median != 20 || d.Min != 10 || d.Max != 30 || math.Abs(d.StdDev-math.Sqrt(200.0/3)) > 1e-9 {
		t.Fatal(d)
	}
}

func TestReadPhasesExcludeEarlierValidationAndUseMeasuredBytes(t *testing.T) {
	d, e := btree.Open(t.TempDir(), btree.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	oracle := map[string][]byte{}
	for i := 0; i < 64; i++ {
		k, v := key(i, false), value(i, 1)
		if e = d.Put(k, v); e != nil {
			t.Fatal(e)
		}
		oracle[string(k)] = v
	}
	if _, e = d.Check(); e != nil {
		t.Fatal(e)
	}
	s, e := d.Stats()
	if e != nil {
		t.Fatal(e)
	}
	phases, e := measureReads(d, oracle, 64)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range phases[:2] {
		want := uint64(r.Queries * s.Height * btree.PageSize)
		if r.IO.ReadBytes != want || r.IO.VerifiedBytes != 0 || r.IO.Syncs != 0 {
			t.Fatal("query phase contains prior checking or unrelated I/O", r, want)
		}
		if r.BytesPerQuery == nil || *r.BytesPerQuery != float64(s.Height*btree.PageSize) {
			t.Fatal("bytes/query denominator", r)
		}
	}
	if phases[0].UsefulBytes != uint64(func() int {
		n := 0
		for k, v := range oracle {
			n += len(k) + len(v)
		}
		return n
	}()) {
		t.Fatal("hit result-byte denominator")
	}
}
