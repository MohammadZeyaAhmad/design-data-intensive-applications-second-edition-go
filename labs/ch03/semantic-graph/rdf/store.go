package rdf

import (
	"sort"
	"strings"
	"sync"
)

const MaxTriples = 32768

// Store keeps three single-component indexes. Match chooses the most selective
// bound component and checks the remaining components; it never joins by label.
type Store struct {
	mu                            sync.RWMutex
	all                           map[Triple]struct{}
	subjects, predicates, objects map[Term]map[Triple]struct{}
}

func New() *Store {
	return &Store{all: map[Triple]struct{}{}, subjects: map[Term]map[Triple]struct{}{}, predicates: map[Term]map[Triple]struct{}{}, objects: map[Term]map[Triple]struct{}{}}
}
func (s *Store) Add(t Triple) (bool, error) {
	if !t.Valid() {
		return false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.all[t]; ok {
		return false, nil
	}
	if len(s.all) >= MaxTriples {
		return false, ErrBudget
	}
	s.all[t] = struct{}{}
	for i, index := range []map[Term]map[Triple]struct{}{s.subjects, s.predicates, s.objects} {
		key := []Term{t.Subject, t.Predicate, t.Object}[i]
		if index[key] == nil {
			index[key] = map[Triple]struct{}{}
		}
		index[key][t] = struct{}{}
	}
	return true, nil
}
func (s *Store) Match(subject, predicate, object *Term) ([]Triple, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidates := s.all
	for i, term := range []*Term{subject, predicate, object} {
		if term == nil {
			continue
		}
		index := []map[Term]map[Triple]struct{}{s.subjects, s.predicates, s.objects}[i]
		subset := index[*term]
		if len(subset) < len(candidates) {
			candidates = subset
		}
	}
	out := []Triple{}
	for t := range candidates {
		if subject != nil && t.Subject != *subject || predicate != nil && t.Predicate != *predicate || object != nil && t.Object != *object {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, len(candidates)
}
func (t Triple) String() string    { s, _ := t.NT(); return s }
func (s *Store) Triples() []Triple { out, _ := s.Match(nil, nil, nil); return out }
func (s *Store) NTriples() string {
	var b strings.Builder
	for _, t := range s.Triples() {
		b.WriteString(t.String())
		b.WriteByte('\n')
	}
	return b.String()
}
