package datalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFixedPointAndDeltaEvaluation(t *testing.T) {
	p, e := Parse(`edge(0,1). edge(1,2). edge(2,0). edge(0,1).
 reach(X,Y) :- edge(X,Y).
 reach(X,Z) :- reach(X,Y), edge(Y,Z).
 same(X) :- reach(X,X).
 both(X,Y) :- reach(X,Y), reach(Y,X).`)
	if e != nil {
		t.Fatal(e)
	}
	var want []Fact
	for _, semi := range []bool{false, true} {
		r, e := Evaluate(context.Background(), p, nil, DefaultLimits(), semi)
		if e != nil {
			t.Fatal(e)
		}
		facts, _ := r.Facts("reach")
		if len(facts) != 9 {
			t.Fatalf("cycle closure: %v", facts)
		}
		if want != nil && !reflect.DeepEqual(facts, want) {
			t.Fatal("delta disagrees with reference")
		}
		want, _ = r.Facts("reach")
		same, _ := r.Facts("same")
		both, _ := r.Facts("both")
		if len(same) != 3 || len(both) != 9 || r.Stats.Rounds > 8 {
			t.Fatalf("fixed point: %+v", r.Stats)
		}
		facts[0].Values[0] = String("mutated")
		copy, _ := r.Facts("reach")
		if !reflect.DeepEqual(copy, want) {
			t.Fatal("result escaped mutable storage")
		}
	}
}

func TestSemiNaiveReducesRepeatedJoinWork(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 18; i++ {
		fmt.Fprintf(&b, "edge(%d,%d).", i, i+1)
	}
	b.WriteString(`reach(X,Y) :- edge(X,Y). reach(X,Z) :- reach(X,Y),edge(Y,Z).`)
	p, e := Parse(b.String())
	if e != nil {
		t.Fatal(e)
	}
	a, e := Evaluate(context.Background(), p, nil, DefaultLimits(), false)
	if e != nil {
		t.Fatal(e)
	}
	d, e := Evaluate(context.Background(), p, nil, DefaultLimits(), true)
	if e != nil {
		t.Fatal(e)
	}
	af, _ := a.Facts("reach")
	df, _ := d.Facts("reach")
	if !reflect.DeepEqual(af, df) || d.Stats.Candidates >= a.Stats.Candidates {
		t.Fatalf("naive %+v delta %+v", a.Stats, d.Stats)
	}
}

func TestTypedConstantsAndDuplicateFacts(t *testing.T) {
	p, e := Parse(`v(1). v("1"). v(<urn:one>). v(true). v(1). same(X) :- v(X),v(X).`)
	if e != nil {
		t.Fatal(e)
	}
	r, e := Evaluate(context.Background(), p, []Fact{{"v", []Value{Integer(1)}}}, DefaultLimits(), true)
	if e != nil {
		t.Fatal(e)
	}
	f, _ := r.Facts("same")
	if len(f) != 4 {
		t.Fatal(f)
	}
}

func TestNonlinearAndMutualRecursion(t *testing.T) {
	p, e := Parse(`r(0,1). r(1,2). r(2,3). r(X,Z) :- r(X,Y),r(Y,Z). a(X,Y) :- r(X,Y). b(X,Y) :- a(X,Y). a(X,Y) :- b(X,Y).`)
	if e != nil {
		t.Fatal(e)
	}
	for _, semi := range []bool{false, true} {
		r, e := Evaluate(context.Background(), p, nil, DefaultLimits(), semi)
		if e != nil {
			t.Fatal(e)
		}
		for _, name := range []string{"r", "a", "b"} {
			f, _ := r.Facts(name)
			if len(f) != 6 {
				t.Fatalf("%s closure %v", name, f)
			}
		}
	}
}

func TestLanguageRejectionAndBounds(t *testing.T) {
	for _, s := range []string{`p(X).`, `p(X) :- q(Y).`, `p(1). p(1,2).`, `p(X) :- not q(X).`, `p(f(X)).`, `p(X) :- q(X), X > 2.`, `p(count(X)) :- q(X).`, `p(X) :- q(X); r(X).`, `p().`} {
		if _, e := Parse(s); e == nil {
			t.Fatalf("accepted unsupported/unsafe %s", s)
		}
	}
	p, e := Parse(`edge(0,1). edge(1,2). reach(X,Y) :- edge(X,Y). reach(X,Z) :- reach(X,Y),edge(Y,Z).`)
	if e != nil {
		t.Fatal(e)
	}
	for _, limits := range []Limits{{1, 10, 1000}, {100, 1, 1000}, {100, 10, 1}} {
		r, e := Evaluate(context.Background(), p, nil, limits, true)
		if !errors.Is(e, ErrBudget) || r.tables != nil {
			t.Fatalf("partial result %+v %v", r, e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := Evaluate(ctx, p, nil, DefaultLimits(), true); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	p.Schema = map[string]int{}
	if _, e := Evaluate(context.Background(), p, nil, DefaultLimits(), true); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}

func FuzzPositiveProgram(f *testing.F) {
	for _, s := range []string{`edge(1,2). reach(X,Y) :- edge(X,Y).`, `p(X) :- not q(X).`, `p("你好").`, `p(<urn:atlas:x>).`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 2048 {
			t.Skip()
		}
		p, e := Parse(s)
		if e != nil {
			return
		}
		limits := Limits{256, 32, 20000}
		a, ae := Evaluate(context.Background(), p, nil, limits, false)
		b, be := Evaluate(context.Background(), p, nil, limits, true)
		if ae != nil || be != nil {
			return
		}
		for name := range p.Schema {
			x, _ := a.Facts(name)
			y, _ := b.Facts(name)
			if !reflect.DeepEqual(x, y) {
				t.Fatalf("delta mismatch %s", name)
			}
		}
	})
}
