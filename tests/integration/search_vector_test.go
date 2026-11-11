//go:build integration && searchvector

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	searchbench "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	searchdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	searchembed "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/embed"
	searchpublish "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/publish"
)

func vectorStore(t *testing.T) *searchdb.Store {
	t.Helper()
	if e := searchdb.EnsureVector(context.Background(), pool); e != nil {
		t.Fatal(e)
	}
	if e := searchdb.CheckVector(context.Background(), pool); e != nil {
		t.Fatal(e)
	}
	return &searchdb.Store{Pool: pool, VectorEnabled: true}
}
func TestSearchVectorRealVersionedRepresentations(t *testing.T) {
	ctx := context.Background()
	s := vectorStore(t)
	owner := tenant(t)
	d := searchDoc(t, s, owner, "doc", domain.Document{ID: "mouse", Title: "Wireless pointing device", Body: "Synthetic feature fixture"})
	embedding := domain.Embedding{ID: d.ID, DocumentVersion: d.Version, Model: "atlas-synthetic", ModelVersion: "1", Dimensions: 8, Values: []float64{1, 0, 0, 0, 0, 0, 0, 0}}
	h := searchembed.Handler{Store: s}
	first, replay, e := h.Handle(ctx, owner, "first", embedding)
	if e != nil || replay || first.Version != 1 {
		t.Fatal(e, first)
	}
	again, replay, e := h.Handle(ctx, owner, "first", embedding)
	if e != nil || !replay || again.Version != 1 {
		t.Fatal("embedding replay", e)
	}
	changed := embedding
	changed.Values = []float64{0, 1, 0, 0, 0, 0, 0, 0}
	if _, _, e = h.Handle(ctx, owner, "first", changed); !errors.Is(e, commands.ErrConflict) {
		t.Fatal("embedding payload collision", e)
	}
	hits, e := s.Vector(ctx, owner, embedding.Model, embedding.ModelVersion, embedding.Values, 10, "cosine")
	if e != nil || len(hits) != 1 || hits[0].Distance != 0 || hits[0].EmbeddingVersion != 1 {
		t.Fatal(hits, e)
	}
	d.Title = "Changed textual meaning"
	d = searchDoc(t, s, owner, "revise", d)
	hits, e = s.Vector(ctx, owner, embedding.Model, embedding.ModelVersion, embedding.Values, 10, "cosine")
	if e != nil || len(hits) != 0 {
		t.Fatal("stale embedding visible", hits, e)
	}
	embedding.Version = 1
	if _, _, e = h.Handle(ctx, owner, "stale-source", embedding); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("stale document accepted", e)
	}
	embedding.DocumentVersion = d.Version
	refreshed, _, e := h.Handle(ctx, owner, "refresh", embedding)
	if e != nil || refreshed.Version != 2 {
		t.Fatal(e)
	}
	other := embedding
	other.Version = 0
	other.ModelVersion = "2"
	if _, _, e = h.Handle(ctx, owner, "new-model", other); e != nil {
		t.Fatal(e)
	}
	hits, e = s.Vector(ctx, owner, embedding.Model, "missing", embedding.Values, 10, "euclidean")
	if e != nil || len(hits) != 0 {
		t.Fatal("model version mixed", hits, e)
	}
	if _, _, e = (searchpublish.Handler{Store: s}).Delete(ctx, owner, "delete", d.ID, d.Version); e != nil {
		t.Fatal(e)
	}
	hits, e = s.Vector(ctx, owner, embedding.Model, "1", embedding.Values, 10, "cosine")
	if e != nil || len(hits) != 0 {
		t.Fatal("deleted document embedding visible", hits, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE search.embedding_history SET metadata='{}' WHERE tenant_id=$1`, owner); e == nil {
		t.Fatal("embedding history mutable")
	}
}
func TestSearchVectorRealConcurrentRevisionAndTenant(t *testing.T) {
	ctx := context.Background()
	s := vectorStore(t)
	a, b := tenant(t), tenant(t)
	for _, owner := range []string{a, b} {
		searchDoc(t, s, owner, "doc", domain.Document{ID: "same-id", Title: "Document", Body: "Per-tenant fixture"})
	}
	v := domain.Embedding{ID: "same-id", DocumentVersion: 1, Model: "atlas-synthetic", ModelVersion: "1", Dimensions: 8, Values: []float64{1, 0, 0, 0, 0, 0, 0, 0}}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, key := range []string{"a", "b"} {
		key := key
		wg.Add(1)
		go func() { defer wg.Done(); _, _, e := (searchembed.Handler{Store: s}).Handle(ctx, a, key, v); errs <- e }()
	}
	wg.Wait()
	close(errs)
	ok, conflict := 0, 0
	for e := range errs {
		if e == nil {
			ok++
		} else if errors.Is(e, domain.ErrConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatal("concurrent first-publication fence", ok, conflict)
	}
	hits, e := s.Vector(ctx, b, v.Model, v.ModelVersion, v.Values, 10, "cosine")
	if e != nil || len(hits) != 0 {
		t.Fatal("tenant vector leak", hits, e)
	}
	if _, _, e = (searchembed.Handler{Store: s}).Handle(ctx, b, "own", v); e != nil {
		t.Fatal(e)
	}
	hits, e = s.Vector(ctx, b, v.Model, v.ModelVersion, v.Values, 10, "euclidean")
	if e != nil || len(hits) != 1 || hits[0].ID != "same-id" {
		t.Fatal(hits, e)
	}
}
func TestSearchVectorRealRecallAndPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r, e := searchbench.Run(ctx, pool, 512, 12, 3)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Plans) != 6 || len(r.Samples) != 216 {
		t.Fatal("incomplete recall experiment")
	}
	for _, s := range r.Samples {
		if s.Recall < 0 || s.Recall > 1 || s.Rows > r.K || s.Rows == 0 {
			t.Fatal("invalid recall sample", s)
		}
	}
	path := os.Getenv("ATLAS_SEARCH_VECTOR_REPORT")
	if path != "" {
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		b, e := json.MarshalIndent(r, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(b, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
