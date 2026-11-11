// Package datalog evaluates a bounded function-free positive Datalog language.
// Unsupported negation, functions, arithmetic and aggregation are errors.
package datalog

import (
	"context"
	"errors"
	"fmt"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxArity = 8

var ErrInvalid = errors.New("invalid Datalog program or fact")
var ErrUnsupported = errors.New("unsupported Datalog language feature")
var ErrBudget = errors.New("Datalog resource bound exceeded; no result")
var predName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
var varName = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]{0,31}$`)

type Value struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

func String(s string) Value { return Value{"string", s} }
func IRI(s string) Value    { return Value{"iri", s} }
func Integer(n int64) Value { return Value{"integer", strconv.FormatInt(n, 10)} }
func Boolean(v bool) Value  { return Value{"boolean", strconv.FormatBool(v)} }
func (v Value) valid() bool {
	if len(v.Text) > 1024 || !utf8.ValidString(v.Text) || strings.ContainsRune(v.Text, 0) {
		return false
	}
	switch v.Kind {
	case "string":
		return true
	case "iri":
		return rdf.ValidIRI(v.Text)
	case "integer":
		n, e := strconv.ParseInt(v.Text, 10, 64)
		return e == nil && strconv.FormatInt(n, 10) == v.Text
	case "boolean":
		return v.Text == "true" || v.Text == "false"
	}
	return false
}

type Term struct {
	Variable string
	Value    Value
}
type Atom struct {
	Name  string
	Terms []Term
}
type Rule struct {
	Head Atom
	Body []Atom
}
type Fact struct {
	Name   string
	Values []Value
}
type Program struct {
	Facts  []Fact
	Rules  []Rule
	Schema map[string]int
}
type Limits struct{ MaxFacts, MaxRounds, MaxWork int }

func DefaultLimits() Limits { return Limits{32768, 1024, 2000000} }

type Stats struct {
	Rounds     int    `json:"rounds"`
	Facts      int    `json:"facts"`
	Derived    int    `json:"derived_facts"`
	Candidates int    `json:"candidate_rows"`
	Strategy   string `json:"strategy"`
}
type Row struct{ Values [MaxArity]Value }
type relation struct {
	arity   int
	rows    map[Row]struct{}
	indexes [MaxArity]map[Value]map[Row]struct{}
}

func newRelation(arity int) *relation {
	r := &relation{arity: arity, rows: map[Row]struct{}{}}
	for i := 0; i < arity; i++ {
		r.indexes[i] = map[Value]map[Row]struct{}{}
	}
	return r
}
func (r *relation) add(row Row) bool {
	if _, ok := r.rows[row]; ok {
		return false
	}
	r.rows[row] = struct{}{}
	for i := 0; i < r.arity; i++ {
		v := row.Values[i]
		if r.indexes[i][v] == nil {
			r.indexes[i][v] = map[Row]struct{}{}
		}
		r.indexes[i][v][row] = struct{}{}
	}
	return true
}

type Result struct {
	tables map[string]*relation
	Stats  Stats
}

func (r Result) Facts(name string) ([]Fact, error) {
	table, ok := r.tables[name]
	if !ok {
		return nil, ErrInvalid
	}
	out := make([]Fact, 0, len(table.rows))
	for row := range table.rows {
		v := make([]Value, table.arity)
		copy(v, row.Values[:table.arity])
		out = append(out, Fact{name, v})
	}
	sortFacts(out)
	return out, nil
}
func validFact(f Fact, schema map[string]int) error {
	if !predName.MatchString(f.Name) || f.Name == "not" || len(f.Values) == 0 || len(f.Values) > MaxArity {
		return ErrInvalid
	}
	if n, ok := schema[f.Name]; ok && n != len(f.Values) {
		return fmt.Errorf("arity conflict for %s: %w", f.Name, ErrInvalid)
	}
	for _, v := range f.Values {
		if !v.valid() {
			return ErrInvalid
		}
	}
	schema[f.Name] = len(f.Values)
	return nil
}
func validateProgram(p Program) error {
	if len(p.Rules) > 32 || len(p.Facts) > 32768 || len(p.Schema) > 64 {
		return ErrBudget
	}
	schema := map[string]int{}
	for _, f := range p.Facts {
		if e := validFact(f, schema); e != nil {
			return e
		}
	}
	check := func(a Atom) error {
		if !predName.MatchString(a.Name) || a.Name == "not" || len(a.Terms) == 0 || len(a.Terms) > MaxArity {
			return ErrInvalid
		}
		if n, ok := schema[a.Name]; ok && n != len(a.Terms) {
			return ErrInvalid
		}
		schema[a.Name] = len(a.Terms)
		for _, t := range a.Terms {
			if t.Variable != "" {
				if !varName.MatchString(t.Variable) || t.Value.Kind != "" || t.Value.Text != "" {
					return ErrInvalid
				}
			} else if !t.Value.valid() {
				return ErrInvalid
			}
		}
		return nil
	}
	for _, r := range p.Rules {
		if len(r.Body) == 0 || len(r.Body) > 8 {
			return ErrInvalid
		}
		if e := check(r.Head); e != nil {
			return e
		}
		bound := map[string]bool{}
		for _, a := range r.Body {
			if e := check(a); e != nil {
				return e
			}
			for _, t := range a.Terms {
				if t.Variable != "" {
					bound[t.Variable] = true
				}
			}
		}
		if len(bound) > 32 {
			return ErrBudget
		}
		for _, t := range r.Head.Terms {
			if t.Variable != "" && !bound[t.Variable] {
				return fmt.Errorf("unsafe head variable %s: %w", t.Variable, ErrInvalid)
			}
		}
	}
	if len(schema) > 64 {
		return ErrBudget
	}
	if len(p.Schema) != len(schema) {
		return ErrInvalid
	}
	for name, arity := range p.Schema {
		if schema[name] != arity {
			return ErrInvalid
		}
	}
	return nil
}
func Evaluate(ctx context.Context, p Program, seeds []Fact, limits Limits, semiNaive bool) (Result, error) {
	return evaluate(ctx, p, seeds, limits, semiNaive)
}
