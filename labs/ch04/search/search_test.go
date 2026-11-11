package search

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func testDoc(i int) Document {
	v := make([]float64, 8)
	for j := range v {
		v[j] = math.Sin(float64((i+1)*(j+3))) + 0.01*float64(j+1)
	}
	return Document{Tenant: []string{"acme", "globex"}[i%2], ID: fmt.Sprintf("sku-%04d", i), Version: 1, Title: []string{"Wireless mouse", "Garden tools", "Warehouse gloves", "Computer cable"}[i%4], Body: "Durable stock for warehouse delivery", Point: &Point{float64(i*37%360) - 180, float64(i*19%180) - 90}, Embedding: &Embedding{"atlas-synthetic", "1", 8, v}}
}
func TestTextPhraseFuzzyAndTenantStatistics(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "docs.bolt"), Options{})
	must(t, e)
	defer s.Close()
	docs := []Document{{Tenant: "acme", ID: "a", Version: 1, Title: "Wireless mouse", Body: "quiet ergonomic computer mouse"}, {Tenant: "acme", ID: "b", Version: 1, Title: "Field mouse", Body: "small animal in a wireless sensor field"}, {Tenant: "globex", ID: "private", Version: 1, Title: "Wireless mouse", Body: "private warehouse"}, {Tenant: "acme", ID: "c", Version: 1, Title: "wireless", Body: "mouse"}}
	for _, d := range docs {
		must(t, s.Put(d))
	}
	r, e := s.Text(TextQuery{"acme", "wireless mouse", Phrase, 10, 0})
	must(t, e)
	if len(r.Hits) != 1 || r.Hits[0].ID != "a" {
		t.Fatal("phrase crossed fields, word order or tenant", r)
	}
	r, e = s.Text(TextQuery{"acme", "wirless", All, 10, 1})
	must(t, e)
	if len(r.Corrections) != 1 || !reflect.DeepEqual(r.Corrections[0].Terms, []string{"wireless"}) || len(r.Hits) != 3 {
		t.Fatal(r)
	}
	before, e := s.Text(TextQuery{"acme", "mouse", All, 10, 0})
	must(t, e)
	for i := 0; i < 100; i++ {
		d := docs[2]
		d.ID = fmt.Sprintf("other-%d", i)
		must(t, s.Put(d))
	}
	after, e := s.Text(TextQuery{"acme", "mouse", All, 10, 0})
	must(t, e)
	if !reflect.DeepEqual(before.Hits, after.Hits) {
		t.Fatal("another tenant changed scores")
	}
	d := docs[0]
	d.Version = 2
	d.Title = "Keyboard"
	d.Body = "Mechanical keys"
	must(t, s.Put(d))
	r, e = s.Text(TextQuery{"acme", "quiet", All, 10, 0})
	must(t, e)
	if len(r.Hits) != 0 {
		t.Fatal("old posting survived update")
	}
	must(t, s.Delete("acme", "a", 3))
	if !errors.Is(s.Put(d), ErrVersion) {
		t.Fatal("stale resurrection accepted")
	}
}
func TestKDAndRadiusMatchBruteForceIncludingDateline(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "docs.bolt"), Options{})
	must(t, e)
	defer s.Close()
	var docs []Document
	for i := 0; i < 160; i++ {
		d := testDoc(i)
		must(t, s.Put(d))
		docs = append(docs, d)
	}
	for i, p := range []Point{{180, 0}, {-180, 0}, {179.8, 0}, {-179.8, 0}, {0, 90}, {-50, 90}} {
		d := testDoc(200 + i*2)
		d.Point = &p
		must(t, s.Put(d))
		docs = append(docs, d)
	}
	boxes := []Box{{-180, -90, 180, 90}, {170, -5, -170, 5}, {-20, -40, 80, 40}, {180, 0, 180, 0}}
	for _, box := range boxes {
		got, e := s.Box("acme", box, 100)
		must(t, e)
		var want []Hit
		for _, d := range docs {
			p := d.Point
			lon := p.Lon >= box.MinLon && p.Lon <= box.MaxLon
			if box.MinLon > box.MaxLon {
				lon = p.Lon >= box.MinLon || p.Lon <= box.MaxLon
			}
			if d.Tenant == "acme" && lon && p.Lat >= box.MinLat && p.Lat <= box.MaxLat {
				want = append(want, Hit{ID: d.ID, Version: d.Version})
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i].ID < want[j].ID })
		if len(want) > 100 {
			want = want[:100]
		}
		if !reflect.DeepEqual(got.Hits, want) {
			t.Fatal(box, got.Hits, want)
		}
	}
	for _, center := range []Point{{180, 0}, {0, 90}, {77, 28}} {
		for _, km := range []float64{0, 100, 2000, math.Pi * earthKM} {
			got, e := s.Radius("acme", center, km, 100)
			must(t, e)
			var want []Hit
			for _, d := range docs {
				if d.Tenant != "acme" {
					continue
				}
				distance := Haversine(center, *d.Point)
				if distance <= km+1e-9 {
					want = append(want, Hit{ID: d.ID, Version: d.Version, Distance: distance})
				}
			}
			sort.Slice(want, func(i, j int) bool {
				if want[i].Distance != want[j].Distance {
					return want[i].Distance < want[j].Distance
				}
				return want[i].ID < want[j].ID
			})
			if !reflect.DeepEqual(got.Hits, want) {
				t.Fatal(center, km, len(got.Hits), len(want))
			}
		}
	}
}
func TestHNSWExactRecallUpdatesAndReopen(t *testing.T) {
	for _, metric := range []Metric{Cosine, Euclidean} {
		t.Run(string(metric), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Metric = metric
			path := filepath.Join(t.TempDir(), "docs.bolt")
			s, e := Open(path, Options{Config: &cfg})
			must(t, e)
			for i := 0; i < 240; i++ {
				must(t, s.Put(testDoc(i)))
			}
			rng := rand.New(rand.NewSource(40))
			recall := 0.0
			for q := 0; q < 20; q++ {
				v := make([]float64, 8)
				for j := range v {
					v[j] = rng.Float64()*2 - 1
				}
				want, e := s.Vector("acme", v, 8, 0, false)
				must(t, e)
				got, e := s.Vector("acme", v, 8, 96, true)
				must(t, e)
				recall += Recall(want.Hits, got.Hits)
				for _, h := range got.Hits {
					if !finite(h.Distance) || h.Distance < 0 {
						t.Fatal(h)
					}
					found := false
					for i := 0; i < 240; i += 2 {
						if h.ID == testDoc(i).ID {
							found = true
						}
					}
					if !found {
						t.Fatal("tenant/identity leak", h)
					}
				}
			}
			if recall/20 < 0.95 {
				t.Fatal("unexpectedly low high-ef recall", recall/20)
			}
			v := testDoc(2).Embedding.Values
			before, e := s.Vector("acme", v, 5, 64, true)
			must(t, e)
			must(t, s.Close())
			s, e = Open(path, Options{})
			must(t, e)
			defer s.Close()
			after, e := s.Vector("acme", v, 5, 64, true)
			must(t, e)
			if !reflect.DeepEqual(before.Hits, after.Hits) {
				t.Fatal("rebuild changed results")
			}
			must(t, s.Delete("acme", "sku-0002", 2))
			got, e := s.Vector("acme", v, 10, 96, true)
			must(t, e)
			for _, h := range got.Hits {
				if h.ID == "sku-0002" {
					t.Fatal("deleted vector survived")
				}
			}
			must(t, s.Close())
		})
	}
}
func TestFailuresBoundsAndUnknownOutcome(t *testing.T) {
	stage := ""
	abort := errors.New("injected")
	path := filepath.Join(t.TempDir(), "docs.bolt")
	s, e := Open(path, Options{Failpoint: func(p string) error {
		if p == stage {
			return abort
		}
		return nil
	}})
	must(t, e)
	d := testDoc(0)
	must(t, s.Put(d))
	stage = "document.ready"
	d.Version = 2
	d.Title = "replacement"
	if e = s.Put(d); !errors.Is(e, abort) {
		t.Fatal(e)
	}
	r, e := s.Text(TextQuery{"acme", "wireless", All, 10, 0})
	must(t, e)
	if len(r.Hits) != 1 || r.Hits[0].Version != 1 {
		t.Fatal("rollback changed index")
	}
	for i := 0; i < 100; i++ {
		_, e = s.Text(TextQuery{fmt.Sprintf("missing-%d", i), "x", All, 10, 0})
		must(t, e)
	}
	if len(s.tenants) != 1 {
		t.Fatal("empty queries allocated unbounded tenant cache")
	}
	bad := clone(d)
	bad.Embedding.Values[0] = math.NaN()
	if !errors.Is(s.Put(bad), ErrInvalid) {
		t.Fatal("NaN accepted")
	}
	bad = clone(d)
	bad.Embedding.ModelVersion = "other"
	if !errors.Is(s.Put(bad), ErrModel) {
		t.Fatal("model mismatch accepted")
	}
	stage = "document.committed"
	if e = s.Put(d); !errors.Is(e, kv.ErrRecovery) {
		t.Fatal(e)
	}
	must(t, s.Close())
	s, e = Open(path, Options{})
	must(t, e)
	defer s.Close()
	r, e = s.Text(TextQuery{"acme", "replacement", All, 10, 0})
	must(t, e)
	if len(r.Hits) != 1 || r.Hits[0].Version != 2 {
		t.Fatal("committed outcome lost")
	}
}
func TestEditDistanceAgainstFullDynamicProgram(t *testing.T) {
	ref := func(a, b string) int {
		x, y := []rune(a), []rune(b)
		m := make([][]int, len(x)+1)
		for i := range m {
			m[i] = make([]int, len(y)+1)
			m[i][0] = i
		}
		for j := range m[0] {
			m[0][j] = j
		}
		for i := 1; i <= len(x); i++ {
			for j := 1; j <= len(y); j++ {
				cost := 0
				if x[i-1] != y[j-1] {
					cost = 1
				}
				m[i][j] = min(m[i-1][j]+1, m[i][j-1]+1, m[i-1][j-1]+cost)
			}
		}
		return m[len(x)][len(y)]
	}
	rng := rand.New(rand.NewSource(15))
	for i := 0; i < 1000; i++ {
		a, b := "", ""
		for j := 0; j < rng.Intn(12); j++ {
			a += string(rune('a' + rng.Intn(4)))
		}
		for j := 0; j < rng.Intn(12); j++ {
			b += string(rune('a' + rng.Intn(4)))
		}
		for cutoff := 0; cutoff <= 2; cutoff++ {
			got, want := editDistance(a, b, cutoff), ref(a, b)
			if want <= cutoff && got != want || want > cutoff && got <= cutoff {
				t.Fatal(a, b, cutoff, got, want)
			}
		}
	}
}

func TestCosineSmallFiniteVectorsAndScoreBounds(t *testing.T) {
	a := make([]float64, 8)
	b := make([]float64, 8)
	a[0] = 1e-160
	b[1] = 1e-160
	if !validVector(a, DefaultConfig()) || !validVector(b, DefaultConfig()) {
		t.Fatal("fixture invalid")
	}
	if d := distance(a, b, Cosine); !finite(d) || math.Abs(d-1) > 1e-12 {
		t.Fatal("unstable cosine norm product", d)
	}
}

func FuzzTokenizerAndCosineBounds(f *testing.F) {
	f.Add("Wireless mouse", uint64(1))
	f.Add("café café", uint64(0x3ff0000000000000))
	f.Fuzz(func(t *testing.T, s string, bits uint64) {
		terms, e := Tokenize(s)
		if e == nil {
			if len(terms) > 1024 {
				t.Fatal("token bound")
			}
			for _, term := range terms {
				if !utf8.ValidString(term) || len([]rune(term)) > 64 || term == "" {
					t.Fatal("invalid token")
				}
			}
			if _, e = Tokenize(strings.Join(terms, " ")); e != nil {
				t.Fatal("token normalization not stable", e)
			}
		}
		v := make([]float64, 8)
		v[0] = math.Float64frombits(bits)
		if validVector(v, DefaultConfig()) {
			d := distance(v, v, Cosine)
			if !finite(d) || d < 0 || d > 1e-12 {
				t.Fatal("unstable finite cosine", d)
			}
		}
	})
}
