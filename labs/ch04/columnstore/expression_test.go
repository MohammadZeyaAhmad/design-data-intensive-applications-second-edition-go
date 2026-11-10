package columnstore

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestGrammarRejectsCodeAndResourceAbuse(t *testing.T) {
	bad := []string{"", " ", "units >", "units = '1'", "tenant = 3", "units > 1; os.Exit(0)", "import = 'os'", "units == 3", "exec('sh')", "category = 'unterminated", "units > 9223372036854775808", strings.Repeat("(", 20) + "true" + strings.Repeat(")", 20), strings.Repeat("true OR ", 100) + "false"}
	for _, s := range bad {
		if _, e := Parse(s); e == nil {
			t.Fatal("unsafe/invalid grammar accepted", s)
		}
	}
	for _, s := range []string{"category = 'quote''; os.Exit(1); //'", "category = 'a\x00b'", "NOT (units IS NULL OR units < -1)"} {
		e, err := Parse(s)
		must(t, err)
		if _, err = Generate(e); err != nil {
			t.Fatal(err)
		}
	}
}
func TestSQLUnknownTruthTable(t *testing.T) {
	r := fixture(0)
	for _, c := range []struct {
		filter string
		want   truth
	}{{"units = NULL", unknownValue}, {"units != 3", unknownValue}, {"NOT (units = 0)", unknownValue}, {"units = 1 OR true", trueValue}, {"units = 1 AND false", falseValue}, {"category IS NULL", trueValue}, {"category IS NOT NULL", falseValue}} {
		e, err := Parse(c.filter)
		must(t, err)
		if got := eval(e.root, r.value); got != c.want {
			t.Fatal(c.filter, got, c.want)
		}
	}
}
func TestGeneratedGoActuallyCompilesAndMatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, source := range []string{"units >= 0 AND (category = 'tools' OR category IS NULL)", "NOT (net_cents < 5 OR units = NULL)", "category = 'quote''; os.Exit(1); //' OR units IS NULL"} {
		e, err := Parse(source)
		must(t, err)
		p, err := Compile(ctx, e, t.TempDir())
		must(t, err)
		batch := wireBatch{N: 64, Columns: map[Column][]Value{}}
		for _, c := range e.refs() {
			for i := 0; i < 64; i++ {
				batch.Columns[c] = append(batch.Columns[c], fixture(i).value(c))
			}
		}
		got, cpu, err := p.run(ctx, e, []wireBatch{batch})
		must(t, err)
		var want uint64
		for i := 0; i < 64; i++ {
			if eval(e.root, fixture(i).value) == trueValue {
				want |= uint64(1) << i
			}
		}
		if len(got) != 1 || got[0] != want || cpu < 0 || !strings.Contains(p.Source(), "func predicate") {
			t.Fatal("native predicate differs", got, want)
		}
		must(t, p.Close())
	}
}

func FuzzExpressionParser(f *testing.F) {
	for _, s := range []string{"true", "units >= 0 AND category IS NULL", "NOT (units = NULL)", "category = 'a''b'", "units > 1; import os"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if e, err := Parse(s); err == nil {
			if _, err = Generate(e); err != nil {
				t.Fatal("bounded grammar generated invalid Go", err)
			}
			if e.Key() == "" {
				t.Fatal("missing expression identity")
			}
		}
	})
}
