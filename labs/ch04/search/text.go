package search

import (
	"math"
	"sort"
	"unicode"
	"unicode/utf8"
)

type TextMode string

const (
	All    TextMode = "all"
	Any    TextMode = "any"
	Phrase TextMode = "phrase"
)

type TextQuery struct {
	Tenant, Text         string
	Mode                 TextMode
	Limit, FuzzyDistance int
}
type Correction struct {
	Input string   `json:"input"`
	Terms []string `json:"terms"`
}
type TextResult struct {
	Hits        []Hit        `json:"hits"`
	Corrections []Correction `json:"corrections,omitempty"`
	Work        Work         `json:"work"`
}
type termData struct{ positions map[string][]int }
type textIndex struct {
	terms    map[string]*termData
	lengths  map[string]int
	versions map[string]uint64
	average  float64
}

// Tokenize lowercases Unicode letters/numbers and keeps combining marks inside
// an existing token. It does not stem, remove accents or infer language meaning.
func Tokenize(s string) ([]string, error) {
	if len(s) > 4352 || !utf8.ValidString(s) {
		return nil, ErrInvalid
	}
	var terms []string
	var token []rune
	flush := func() {
		if len(token) > 0 {
			terms = append(terms, string(token))
			token = nil
		}
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || len(token) > 0 && unicode.IsMark(r) {
			token = append(token, unicode.ToLower(r))
			if len(token) > 64 {
				return nil, ErrInvalid
			}
		} else {
			flush()
		}
		if len(terms) > 1024 {
			return nil, ErrInvalid
		}
	}
	flush()
	if len(terms) > 1024 {
		return nil, ErrInvalid
	}
	return terms, nil
}
func documentTerms(d Document) (map[string][]int, error) {
	a, e := Tokenize(d.Title)
	if e != nil {
		return nil, e
	}
	b, e := Tokenize(d.Body)
	if e != nil || len(a)+len(b) == 0 || len(a)+len(b) > 1024 {
		return nil, ErrInvalid
	}
	m := map[string][]int{}
	for i, t := range a {
		m[t] = append(m[t], i)
	}
	for i, t := range b {
		m[t] = append(m[t], 1024+i)
	}
	return m, nil
}
func buildText(docs []Document) (*textIndex, error) {
	x := &textIndex{terms: map[string]*termData{}, lengths: map[string]int{}, versions: map[string]uint64{}}
	for _, d := range docs {
		terms, e := documentTerms(d)
		if e != nil {
			return nil, e
		}
		n := 0
		for t, positions := range terms {
			if x.terms[t] == nil {
				if len(x.terms) >= MaxTerms {
					return nil, ErrInvalid
				}
				x.terms[t] = &termData{positions: map[string][]int{}}
			}
			x.terms[t].positions[d.ID] = positions
			n += len(positions)
		}
		x.lengths[d.ID] = n
		x.versions[d.ID] = d.Version
		x.average += float64(n)
	}
	if len(docs) > 0 {
		x.average /= float64(len(docs))
	}
	return x, nil
}
func editDistance(a, b string, cutoff int) int {
	x, y := []rune(a), []rune(b)
	if len(x)-len(y) > cutoff || len(y)-len(x) > cutoff {
		return cutoff + 1
	}
	previous := make([]int, len(y)+1)
	for j := range previous {
		previous[j] = j
	}
	for i, r := range x {
		next := make([]int, len(y)+1)
		next[0] = i + 1
		low := next[0]
		for j := range y {
			next[j+1] = cutoff + 1
		}
		for j := max(0, i-cutoff); j < min(len(y), i+cutoff+1); j++ {
			s := y[j]
			cost := 0
			if r != s {
				cost = 1
			}
			next[j+1] = min(previous[j+1]+1, next[j]+1, previous[j]+cost)
			low = min(low, next[j+1])
		}
		if low > cutoff {
			return cutoff + 1
		}
		previous = next
	}
	return previous[len(y)]
}
func (x *textIndex) expand(term string, cutoff int) []string {
	if x.terms[term] != nil || cutoff == 0 {
		return []string{term}
	}
	type match struct {
		term  string
		edits int
	}
	var matches []match
	for word := range x.terms {
		if n := editDistance(term, word, cutoff); n <= cutoff {
			matches = append(matches, match{word, n})
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].edits != matches[j].edits {
			return matches[i].edits < matches[j].edits
		}
		return matches[i].term < matches[j].term
	})
	if len(matches) > 4 {
		matches = matches[:4]
	}
	var result []string
	for _, m := range matches {
		result = append(result, m.term)
	}
	return result
}
func (x *textIndex) query(q TextQuery) (TextResult, error) {
	var result TextResult
	if !namePattern.MatchString(q.Tenant) || len(q.Text) > 256 || q.Limit < 1 || q.Limit > 100 || q.Mode != All && q.Mode != Any && q.Mode != Phrase || q.FuzzyDistance < 0 || q.FuzzyDistance > 2 || q.Mode == Phrase && q.FuzzyDistance != 0 {
		return result, ErrInvalid
	}
	tokens, e := Tokenize(q.Text)
	if e != nil || len(tokens) < 1 || len(tokens) > 16 || q.FuzzyDistance > 0 && len(tokens) > 4 {
		return result, ErrInvalid
	}
	result.Work.Documents = len(x.lengths)
	groups := make([][]string, len(tokens))
	var candidates map[string]bool
	for i, token := range tokens {
		groups[i] = x.expand(token, q.FuzzyDistance)
		if q.FuzzyDistance > 0 && x.terms[token] == nil {
			result.Corrections = append(result.Corrections, Correction{token, groups[i]})
		}
		set := map[string]bool{}
		for _, term := range groups[i] {
			if data := x.terms[term]; data != nil {
				for id := range data.positions {
					set[id] = true
					result.Work.Postings++
				}
			}
		}
		if i == 0 {
			candidates = set
		} else if q.Mode == Any {
			for id := range set {
				candidates[id] = true
			}
		} else {
			for id := range candidates {
				if !set[id] {
					delete(candidates, id)
				}
			}
		}
	}
	result.Work.Candidates = len(candidates)
	for id := range candidates {
		if q.Mode == Phrase && !x.phrase(id, tokens) {
			continue
		}
		score := 0.0
		seen := map[string]bool{}
		for i, group := range groups {
			for _, term := range group {
				if seen[term] {
					continue
				}
				seen[term] = true
				data := x.terms[term]
				if data == nil {
					continue
				}
				positions := data.positions[id]
				if len(positions) == 0 {
					continue
				}
				tf := 0.0
				for _, p := range positions {
					if p < 1024 {
						tf += 2
					} else {
						tf++
					}
				}
				df, n := float64(len(data.positions)), float64(len(x.lengths))
				idf := math.Log(1 + (n-df+0.5)/(df+0.5))
				denom := tf + 1.2*(1-0.75+0.75*float64(x.lengths[id])/x.average)
				penalty := 1.0 / float64(1+editDistance(tokens[i], term, 2))
				score += idf * tf * 2.2 / denom * penalty
			}
		}
		result.Hits = append(result.Hits, Hit{ID: id, Version: x.versions[id], Score: score})
	}
	sort.Slice(result.Hits, func(i, j int) bool {
		a, b := result.Hits[i], result.Hits[j]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.ID < b.ID
	})
	if len(result.Hits) > q.Limit {
		result.Hits = result.Hits[:q.Limit]
	}
	return result, nil
}
func (x *textIndex) phrase(id string, tokens []string) bool {
	first := x.terms[tokens[0]]
	if first == nil {
		return false
	}
	for _, start := range first.positions[id] {
		ok := true
		for offset, t := range tokens[1:] {
			data := x.terms[t]
			if data == nil {
				ok = false
				break
			}
			positions := data.positions[id]
			want := start + offset + 1
			j := sort.SearchInts(positions, want)
			if j == len(positions) || positions[j] != want {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
