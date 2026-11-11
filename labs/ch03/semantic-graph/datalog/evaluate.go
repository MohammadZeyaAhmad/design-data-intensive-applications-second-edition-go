package datalog

import (
	"context"
	"sort"
)

func sortFacts(facts []Fact) {
	sort.Slice(facts, func(i, j int) bool {
		a, b := facts[i], facts[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		for k := range a.Values {
			if a.Values[k].Kind != b.Values[k].Kind {
				return a.Values[k].Kind < b.Values[k].Kind
			}
			if a.Values[k].Text != b.Values[k].Text {
				return a.Values[k].Text < b.Values[k].Text
			}
		}
		return false
	})
}
func evaluate(ctx context.Context, p Program, seeds []Fact, limits Limits, semi bool) (Result, error) {
	zero := Result{}
	if len(seeds) > 262144 {
		return zero, ErrBudget
	}
	if e := ctx.Err(); e != nil {
		return zero, e
	}
	if e := validateProgram(p); e != nil {
		return zero, e
	}
	if limits.MaxFacts < 1 || limits.MaxFacts > 262144 || limits.MaxRounds < 1 || limits.MaxRounds > 4096 || limits.MaxWork < 1 || limits.MaxWork > 20000000 {
		return zero, ErrInvalid
	}
	schema := map[string]int{}
	for k, v := range p.Schema {
		schema[k] = v
	}
	for _, f := range append(append([]Fact{}, p.Facts...), seeds...) {
		if e := validFact(f, schema); e != nil {
			return zero, e
		}
	}
	if len(schema) > 64 {
		return zero, ErrBudget
	}
	all := map[string]*relation{}
	delta := map[string]*relation{}
	for name, arity := range schema {
		all[name] = newRelation(arity)
		delta[name] = newRelation(arity)
	}
	count := 0
	for _, f := range append(append([]Fact{}, p.Facts...), seeds...) {
		var row Row
		copy(row.Values[:], f.Values)
		if all[f.Name].add(row) {
			delta[f.Name].add(row)
			count++
			if count > limits.MaxFacts {
				return zero, ErrBudget
			}
		}
	}
	initial := count
	stats := Stats{Strategy: "naive"}
	if semi {
		stats.Strategy = "semi-naive"
	}
	for round := 0; ; round++ {
		if e := ctx.Err(); e != nil {
			return zero, e
		}
		if round >= limits.MaxRounds {
			return zero, ErrBudget
		}
		next := map[string]*relation{}
		for name, arity := range schema {
			next[name] = newRelation(arity)
		}
		added := 0
		for _, rule := range p.Rules {
			positions := []int{-1}
			if semi {
				positions = nil
				for i, a := range rule.Body {
					if len(delta[a.Name].rows) > 0 {
						positions = append(positions, i)
					}
				}
			}
			for _, deltaPosition := range positions {
				var join func(int, map[string]Value) error
				join = func(at int, bindings map[string]Value) error {
					if e := ctx.Err(); e != nil {
						return e
					}
					if at == len(rule.Body) {
						var row Row
						for i, t := range rule.Head.Terms {
							if t.Variable != "" {
								row.Values[i] = bindings[t.Variable]
							} else {
								row.Values[i] = t.Value
							}
						}
						head := all[rule.Head.Name]
						if _, exists := head.rows[row]; !exists && next[rule.Head.Name].add(row) {
							added++
							if count+added > limits.MaxFacts {
								return ErrBudget
							}
						}
						return nil
					}
					atom := rule.Body[at]
					rel := all[atom.Name]
					if at == deltaPosition {
						rel = delta[atom.Name]
					}
					candidates := rel.rows
					for i, t := range atom.Terms {
						v, bound := t.Value, t.Variable == ""
						if t.Variable != "" {
							v, bound = bindings[t.Variable]
						}
						if bound {
							matches := rel.indexes[i][v]
							if len(matches) < len(candidates) {
								candidates = matches
							}
						}
					}
					for row := range candidates {
						stats.Candidates++
						if stats.Candidates > limits.MaxWork {
							return ErrBudget
						}
						nextBindings := make(map[string]Value, len(bindings)+len(atom.Terms))
						for k, v := range bindings {
							nextBindings[k] = v
						}
						matches := true
						for i, t := range atom.Terms {
							v := row.Values[i]
							if t.Variable == "" {
								if t.Value != v {
									matches = false
									break
								}
							} else if old, ok := nextBindings[t.Variable]; ok && old != v {
								matches = false
								break
							} else {
								nextBindings[t.Variable] = v
							}
						}
						if matches {
							if e := join(at+1, nextBindings); e != nil {
								return e
							}
						}
					}
					return nil
				}
				if e := join(0, map[string]Value{}); e != nil {
					return zero, e
				}
			}
		}
		stats.Rounds = round + 1
		if added == 0 {
			stats.Facts = count
			stats.Derived = count - initial
			return Result{tables: all, Stats: stats}, nil
		}
		for name, rel := range next {
			for row := range rel.rows {
				all[name].add(row)
			}
		}
		count += added
		delta = next
	}
}
