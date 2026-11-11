package sparql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	semantic "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"strings"
	"sync"
	"time"
)

func nonce() (string, error) {
	var b [16]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}

type Store struct {
	mu       sync.RWMutex
	tracking sync.Mutex
	client   *Client
	prefix   string
	sources  map[string]rdf.Term
	owned    map[rdf.Term]bool
	closed   bool
}

func New(client *Client) (*Store, error) {
	id, e := nonce()
	if e != nil {
		return nil, e
	}
	return &Store{client: client, prefix: "urn:atlas:semantic-run:" + id + ":", sources: map[string]rdf.Term{}, owned: map[rdf.Term]bool{}}, nil
}
func (s *Store) Source(tenant string) (rdf.Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sources[tenant]
	if !ok {
		return rdf.Term{}, semantic.ErrInvalid
	}
	return v, nil
}
func (s *Store) Replace(ctx context.Context, f graph.Fixture) error {
	if e := graph.Validate(f); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return graph.ErrClosed
	}
	next := map[string]rdf.Term{}
	for _, n := range f.Nodes {
		next[n.Tenant] = rdf.URI(s.prefix + "source:" + n.Tenant)
	}
	var update strings.Builder
	for _, uri := range s.sources {
		fmt.Fprintf(&update, "DROP SILENT GRAPH %s;\n", uri.String())
	}
	for tenant, uri := range next {
		triples, e := semantic.Triples(f, tenant)
		if e != nil {
			return e
		}
		fmt.Fprintf(&update, "DROP SILENT GRAPH %s; INSERT DATA { GRAPH %s {\n%s} };\n", uri.String(), uri.String(), triples.NTriples())
		s.owned[uri] = true
	}
	if e := s.client.Update(ctx, update.String()); e != nil {
		return e
	}
	s.sources = next
	return nil
}
func projection(source, scratch rdf.Term, q semantic.Query) string {
	owner, _ := rdf.TenantURI(q.Tenant)
	linkFrom, linkTo := "?from", "?to"
	if q.Direction == "in" {
		linkFrom, linkTo = linkTo, linkFrom
	}
	kinds := []string{semantic.Class("route").String(), semantic.Class("supplies").String()}
	if q.Kind != "all" {
		kinds = []string{semantic.Class(q.Kind).String()}
	}
	return fmt.Sprintf(`PREFIX a: <%s> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
INSERT { GRAPH %s { ?node a ?kind; a:owner %s; a:id ?id; rdfs:label ?name } }
WHERE { GRAPH %s { ?node a ?kind; a:owner %s; a:id ?id; rdfs:label ?name . FILTER(?kind IN(a:Warehouse,a:Supplier)) } };
INSERT { GRAPH %s { %s a:link %s } }
WHERE { GRAPH %s { ?edge a ?kind; a:owner %s; a:from ?from; a:to ?to; a:enabled true; a:capacity ?capacity .
?from a:owner %s . ?to a:owner %s . FILTER(?kind IN(%s) && ?capacity >= %d) } };`, rdf.Vocab, scratch.String(), owner.String(), source.String(), owner.String(), scratch.String(), linkFrom, linkTo, source.String(), owner.String(), owner.String(), owner.String(), strings.Join(kinds, ","), q.MinCapacity)
}
func selectQuery(scratch rdf.Term, q semantic.Query) string {
	start, _ := rdf.EntityURI(q.Tenant, "node", q.Start)
	owner, _ := rdf.TenantURI(q.Tenant)
	operator := "+"
	if q.Reflexive {
		operator = "*"
	}
	return fmt.Sprintf(`PREFIX a: <%s> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?vertex ?id ?kind ?name WHERE { GRAPH %s { %s a:owner %s; a ?startKind . FILTER(?startKind IN(a:Warehouse,a:Supplier))
%s a:link%s ?vertex . ?vertex a ?kind; a:owner %s; a:id ?id; rdfs:label ?name . FILTER(?kind IN(a:Warehouse,a:Supplier)) } } ORDER BY ?id`, rdf.Vocab, scratch.String(), start.String(), owner.String(), start.String(), operator, owner.String())
}
func (s *Store) Query(ctx context.Context, q semantic.Query) (semantic.Answer, error) {
	var e error
	q, e = semantic.Normalize(q)
	if e != nil {
		return nil, e
	}
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, graph.ErrClosed
	}
	source, ok := s.sources[q.Tenant]
	if !ok {
		return semantic.Answer{}, nil
	}
	id, e := nonce()
	if e != nil {
		return nil, e
	}
	scratch := rdf.URI(s.prefix + "scratch:" + id)
	s.tracking.Lock()
	s.owned[scratch] = true
	s.tracking.Unlock()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if s.client.Update(cleanup, "DROP SILENT GRAPH "+scratch.String()) == nil {
			s.tracking.Lock()
			delete(s.owned, scratch)
			s.tracking.Unlock()
		}
	}()
	if e = s.client.Update(ctx, projection(source, scratch, q)); e != nil {
		return nil, e
	}
	rows, e := s.client.Select(ctx, selectQuery(scratch, q))
	if e != nil {
		return nil, e
	}
	out := semantic.Answer{}
	seen := map[string]bool{}
	for _, row := range rows {
		v, e := row["vertex"].Term()
		if e != nil {
			return nil, e
		}
		id, e := rdf.NodeID(v, q.Tenant)
		if e != nil {
			return nil, e
		}
		idTerm, e := row["id"].Term()
		if e != nil || idTerm != rdf.String(id) {
			return nil, semantic.ErrInvalid
		}
		kind, e := row["kind"].Term()
		if e != nil {
			return nil, e
		}
		nodeKind := ""
		if kind == semantic.Class("warehouse") {
			nodeKind = "warehouse"
		} else if kind == semantic.Class("supplier") {
			nodeKind = "supplier"
		} else {
			return nil, semantic.ErrInvalid
		}
		name, e := row["name"].Term()
		if e != nil || name.Kind != rdf.Literal || name.Datatype != rdf.XSD+"string" || name.Language != "" || seen[id] {
			return nil, semantic.ErrInvalid
		}
		seen[id] = true
		out = append(out, semantic.Vertex{ID: id, Kind: nodeKind, Name: name.Value})
	}
	return semantic.Canonical(out), nil
}
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var query strings.Builder
	s.tracking.Lock()
	defer s.tracking.Unlock()
	for uri := range s.owned {
		fmt.Fprintf(&query, "DROP SILENT GRAPH %s;\n", uri.String())
	}
	if query.Len() > 0 {
		if e := s.client.Update(ctx, query.String()); e != nil {
			return e
		}
	}
	s.closed = true
	s.sources = nil
	s.owned = nil
	return nil
}
