package search

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/storage"
)

type VectorSample struct {
	Metric              Metric  `json:"metric"`
	M                   int     `json:"m"`
	EfSearch            int     `json:"ef_search"`
	Query               int     `json:"query"`
	Repetition          int     `json:"repetition"`
	Recall              float64 `json:"recall_at_k"`
	ExactNanos          int64   `json:"exact_nanos"`
	ApproxNanos         int64   `json:"ann_nanos"`
	DistanceEvaluations int     `json:"distance_evaluations"`
}
type GraphEvidence struct {
	Metric     Metric     `json:"metric"`
	M          int        `json:"m"`
	BuildNanos int64      `json:"build_nanos"`
	Stats      GraphStats `json:"stats"`
}
type TextEvidence struct {
	Query  string     `json:"query"`
	Mode   TextMode   `json:"mode"`
	Fuzzy  int        `json:"fuzzy_distance"`
	Result TextResult `json:"result"`
}
type SpatialEvidence struct {
	Kind    string `json:"kind"`
	Matched int    `json:"matched"`
	Work    Work   `json:"work"`
}
type Contrast struct {
	Query   string `json:"query"`
	Lexical []Hit  `json:"lexical_hits"`
	Vector  []Hit  `json:"manual_vector_hits"`
	Method  string `json:"embedding_method"`
}
type Report struct {
	Passed           bool              `json:"passed"`
	DatasetRows      int               `json:"dataset_rows"`
	Repetitions      int               `json:"repetitions"`
	Environment      map[string]any    `json:"environment"`
	Configuration    Config            `json:"configuration"`
	Text             []TextEvidence    `json:"text"`
	Spatial          []SpatialEvidence `json:"spatial"`
	Graphs           []GraphEvidence   `json:"graphs"`
	Samples          []VectorSample    `json:"samples"`
	Contrast         Contrast          `json:"lexical_vector_contrast"`
	ReopenVerified   bool              `json:"reopen_verified"`
	MutationVerified bool              `json:"updates_deletes_verified"`
	Definitions      []string          `json:"definitions"`
}

func SyntheticDocument(i int) Document {
	v := make([]float64, 8)
	for j := range v {
		v[j] = math.Sin(float64((i+1)*(j+3))) + 0.01*float64(j+1)
	}
	return Document{Tenant: []string{"acme", "globex"}[i%2], ID: fmt.Sprintf("sku-%05d", i), Version: 1, Title: []string{"Wireless mouse", "Garden tools", "Warehouse gloves", "Computer cable"}[i%4], Body: fmt.Sprintf("Durable stock for warehouse delivery serial%05d", i), Point: &Point{float64(i*37%360) - 180, float64(i*19%180) - 90}, Embedding: &Embedding{"atlas-synthetic", "1", 8, v}}
}
func ids(hits []Hit) []string {
	var out []string
	for _, h := range hits {
		out = append(out, h.ID)
	}
	return out
}
func RunScenarioAt(parent string, rows, repetitions int) (Report, error) {
	var report Report
	if rows < 32 || rows > 4096 || repetitions < 1 || repetitions > 5 {
		return report, ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return report, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-search-")
	if e != nil {
		return report, e
	}
	defer os.RemoveAll(dir)
	config := DefaultConfig()
	report = Report{DatasetRows: rows, Repetitions: repetitions, Configuration: config, Environment: storage.Environment(dir)}
	report.Environment["timing"] = "warm Go query wall clock; lazy derived-index build timed separately; no cache eviction or device ranking"
	path := filepath.Join(dir, "documents.bolt")
	s, e := Open(path, Options{})
	if e != nil {
		return report, e
	}
	defer func() { s.Close() }()
	var docs []Document
	for _, i := range rand.New(rand.NewSource(405)).Perm(rows) {
		d := SyntheticDocument(i)
		if e = s.Put(d); e != nil {
			return report, e
		}
		docs = append(docs, d)
	}
	for _, q := range []TextQuery{{"acme", "wireless mouse", All, 20, 0}, {"acme", "wireless mouse", Phrase, 20, 0}, {"acme", "wirless", All, 20, 1}, {"globex", "garden cable", Any, 20, 0}} {
		if _, e = s.Text(q); e != nil {
			return report, e
		}
		r, e := s.Text(q)
		if e != nil {
			return report, e
		}
		for _, h := range r.Hits {
			found := false
			for _, d := range docs {
				if d.ID == h.ID && d.Tenant == q.Tenant {
					found = true
				}
			}
			if !found {
				return report, errors.New("text query escaped tenant corpus")
			}
		}
		report.Text = append(report.Text, TextEvidence{q.Text, q.Mode, q.FuzzyDistance, r})
	}
	for _, box := range []Box{{170, -30, -170, 30}, {-60, -40, 80, 40}} {
		got, e := s.Box("acme", box, 100)
		if e != nil {
			return report, e
		}
		var want []string
		for _, d := range docs {
			if d.Tenant != "acme" {
				continue
			}
			p := d.Point
			lon := p.Lon >= box.MinLon && p.Lon <= box.MaxLon
			if box.MinLon > box.MaxLon {
				lon = p.Lon >= box.MinLon || p.Lon <= box.MaxLon
			}
			if lon && p.Lat >= box.MinLat && p.Lat <= box.MaxLat {
				want = append(want, d.ID)
			}
		}
		sort.Strings(want)
		if len(want) > 100 {
			want = want[:100]
		}
		if !reflect.DeepEqual(ids(got.Hits), want) {
			return report, errors.New("KD-tree differs from brute-force rectangle")
		}
		report.Spatial = append(report.Spatial, SpatialEvidence{"rectangle", len(got.Hits), got.Work})
	}
	center := Point{179.9, 0}
	radius, e := s.Radius("acme", center, 5000, 100)
	if e != nil {
		return report, e
	}
	var expected []Hit
	for _, d := range docs {
		if d.Tenant == "acme" {
			distance := Haversine(center, *d.Point)
			if distance <= 5000+1e-9 {
				expected = append(expected, Hit{ID: d.ID, Version: d.Version, Distance: distance})
			}
		}
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].Distance != expected[j].Distance {
			return expected[i].Distance < expected[j].Distance
		}
		return expected[i].ID < expected[j].ID
	})
	if len(expected) > 100 {
		expected = expected[:100]
	}
	if !reflect.DeepEqual(radius.Hits, expected) {
		return report, errors.New("radius differs from brute-force spherical distance")
	}
	report.Spatial = append(report.Spatial, SpatialEvidence{"haversine-radius", len(radius.Hits), radius.Work})
	var vectors []Document
	for _, d := range docs {
		if d.Tenant == "acme" {
			vectors = append(vectors, d)
		}
	}
	rng := rand.New(rand.NewSource(405))
	queries := make([][]float64, 16)
	for i := range queries {
		queries[i] = make([]float64, 8)
		for j := range queries[i] {
			queries[i][j] = rng.Float64()*2 - 1
		}
	}
	for _, metric := range []Metric{Cosine, Euclidean} {
		for _, m := range []int{4, 8, 16} {
			c := config
			c.Metric = metric
			c.M = m
			start := time.Now()
			g := buildGraph(append([]Document(nil), vectors...), c)
			buildTime := time.Since(start).Nanoseconds()
			report.Graphs = append(report.Graphs, GraphEvidence{metric, m, buildTime, g.stats()})
			for _, ef := range []int{8, 32, 128} {
				for qi, v := range queries {
					want := exact(vectors, v, 8, c)
					g.query(v, 8, ef)
					for rep := 0; rep < repetitions; rep++ {
						start = time.Now()
						want = exact(vectors, v, 8, c)
						exactTime := time.Since(start).Nanoseconds()
						start = time.Now()
						got := g.query(v, 8, ef)
						annTime := time.Since(start).Nanoseconds()
						for _, h := range got.Hits {
							found := false
							for _, d := range vectors {
								if h.ID == d.ID && h.Version == d.Version {
									found = true
								}
							}
							if !found || !finite(h.Distance) {
								return report, errors.New("ANN returned an ineligible vector")
							}
						}
						report.Samples = append(report.Samples, VectorSample{metric, m, ef, qi, rep, Recall(want.Hits, got.Hits), exactTime, annTime, got.Work.Distances})
					}
				}
			}
		}
	}
	query := SyntheticDocument(2).Embedding.Values
	before, e := s.Vector("acme", query, 8, 64, true)
	if e != nil {
		return report, e
	}
	if e = s.Close(); e != nil {
		return report, e
	}
	s, e = Open(path, Options{})
	if e != nil {
		return report, e
	}
	after, e := s.Vector("acme", query, 8, 64, true)
	if e != nil || !reflect.DeepEqual(before.Hits, after.Hits) {
		return report, errors.New("derived graph rebuild changed results")
	}
	report.ReopenVerified = true
	changed := SyntheticDocument(0)
	changed.Version = 2
	changed.Title = "Replacement wrench"
	changed.Body = "Updated discovery projection"
	changed.Point = &Point{0, 0}
	changed.Embedding.Values = []float64{0, 1, 0, 0, 0, 0, 0, 0}
	if e = s.Put(changed); e != nil {
		return report, e
	}
	r, e := s.Text(TextQuery{"acme", "serial00000", All, 10, 0})
	if e != nil || len(r.Hits) != 0 {
		return report, errors.New("old postings survived mutation")
	}
	if e = s.Delete("acme", changed.ID, 3); e != nil {
		return report, e
	}
	if !errors.Is(s.Put(changed), ErrVersion) {
		return report, errors.New("stale document resurrected")
	}
	ann, e := s.Vector("acme", query, 100, 128, true)
	if e != nil {
		return report, e
	}
	for _, h := range ann.Hits {
		if h.ID == changed.ID {
			return report, errors.New("deleted vector survived")
		}
	}
	report.MutationVerified = true
	contrast, e := manualContrast(filepath.Join(dir, "manual.bolt"))
	if e != nil {
		return report, e
	}
	report.Contrast = contrast
	report.Definitions = []string{"persistent tenant-qualified document/version floors with atomic raw-record publication; postings, spatial tree and HNSW rebuild lazily after mutations/reopen", "inverted positions support all/any/phrase; BM25-style title-weighted scoring uses tenant-local statistics; typo expansion is explicit bounded Levenshtein, not learned language understanding", "KD-tree rectangles include wrapped longitude; radius uses a conservative spherical bounding rectangle and exact Haversine filtering", "all HNSW graphs contain one tenant and one model/version/dimension/metric; exact cosine and Euclidean scans are the reference", "M 4/8/16, efConstruction 64, efSearch 8/32/128; all per-query recall and raw warm timing samples retained; query has no hidden exact fallback", "graph bytes count float64 arrays and string edge-reference headers only; Go maps, allocations and temporary query heaps are excluded from this lower-bound memory estimate", "synthetic/preassigned vectors demonstrate numeric retrieval and a deliberate lexical/vector contrast; no embedding model or natural-language comprehension is implemented", "rebuild cost is separate from warm queries; dataset is bounded, cache uncontrolled and filesystem/device traffic unmeasured"}
	report.Passed = true
	return report, nil
}
func manualContrast(path string) (Contrast, error) {
	s, e := Open(path, Options{})
	if e != nil {
		return Contrast{}, e
	}
	defer s.Close()
	for _, item := range []struct {
		id, title, body string
		v               []float64
	}{{"computer-mouse", "Computer mouse", "Wireless input device", []float64{1, 0, 0, 0, 0, 0, 0, 0}}, {"animal-mouse", "Field mouse", "Small animal", []float64{0, 1, 0, 0, 0, 0, 0, 0}}, {"peripheral", "Ergonomic pointing peripheral", "Quiet wireless input accessory", []float64{0.99, 0.01, 0, 0, 0, 0, 0, 0}}} {
		if e = s.Put(Document{Tenant: "acme", ID: item.id, Version: 1, Title: item.title, Body: item.body, Embedding: &Embedding{"atlas-synthetic", "1", 8, item.v}}); e != nil {
			return Contrast{}, e
		}
	}
	lex, e := s.Text(TextQuery{"acme", "mouse", All, 10, 0})
	if e != nil {
		return Contrast{}, e
	}
	vec, e := s.Vector("acme", []float64{1, 0, 0, 0, 0, 0, 0, 0}, 2, 0, false)
	if e != nil {
		return Contrast{}, e
	}
	if len(lex.Hits) != 2 || len(vec.Hits) != 2 || vec.Hits[1].ID != "peripheral" {
		return Contrast{}, kv.ErrCorrupt
	}
	return Contrast{"mouse", lex.Hits, vec.Hits, "manually assigned topic coordinates; no model inference or semantic-understanding claim"}, nil
}
