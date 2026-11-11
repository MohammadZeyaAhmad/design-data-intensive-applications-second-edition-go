// Package search implements bounded tenant-isolated text, spatial and vector
// indexes over synthetic discovery documents. Persistent records are primary;
// derived indexes rebuild deterministically after changes or reopening.
package search

import (
	"errors"
	"math"
	"regexp"
	"unicode/utf8"
)

const MaxDocuments = 4096
const MaxTerms = 32768

var ErrInvalid = errors.New("invalid search argument")
var ErrVersion = errors.New("document version must advance")
var ErrModel = errors.New("embedding model/version/dimension mismatch")
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

type Metric string

const (
	Cosine    Metric = "cosine"
	Euclidean Metric = "euclidean"
)

type Config struct {
	Model, ModelVersion           string
	Dimensions, M, EfConstruction int
	Metric                        Metric
	Seed                          uint64
}

func DefaultConfig() Config { return Config{"atlas-synthetic", "1", 8, 8, 64, Cosine, 42} }
func (c Config) valid() bool {
	return namePattern.MatchString(c.Model) && namePattern.MatchString(c.ModelVersion) && c.Dimensions >= 2 && c.Dimensions <= 64 && c.M >= 2 && c.M <= 32 && c.EfConstruction >= c.M && c.EfConstruction <= 512 && (c.Metric == Cosine || c.Metric == Euclidean)
}

type Point struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
}

func (p Point) valid() bool {
	return finite(p.Lon) && finite(p.Lat) && p.Lon >= -180 && p.Lon <= 180 && p.Lat >= -90 && p.Lat <= 90
}

type Embedding struct {
	Model        string    `json:"model"`
	ModelVersion string    `json:"model_version"`
	Dimensions   int       `json:"dimensions"`
	Values       []float64 `json:"values"`
}
type Document struct {
	Tenant    string     `json:"tenant"`
	ID        string     `json:"id"`
	Version   uint64     `json:"version"`
	Title     string     `json:"title"`
	Body      string     `json:"body"`
	Point     *Point     `json:"point,omitempty"`
	Embedding *Embedding `json:"embedding,omitempty"`
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func validVector(v []float64, c Config) bool {
	if len(v) != c.Dimensions {
		return false
	}
	norm := 0.0
	for _, x := range v {
		if !finite(x) || math.Abs(x) > 1e6 {
			return false
		}
		norm = math.Hypot(norm, x)
	}
	return c.Metric != Cosine || norm > 0
}
func (d Document) valid(c Config) error {
	if !namePattern.MatchString(d.Tenant) || !namePattern.MatchString(d.ID) || d.Version == 0 || d.Version > 1e12 || len(d.Title) < 1 || len(d.Title) > 256 || len(d.Body) > 4096 || !utf8.ValidString(d.Title) || !utf8.ValidString(d.Body) || d.Point != nil && !d.Point.valid() {
		return ErrInvalid
	}
	if _, e := documentTerms(d); e != nil {
		return e
	}
	if d.Embedding != nil {
		v := d.Embedding
		if v.Model != c.Model || v.ModelVersion != c.ModelVersion || v.Dimensions != c.Dimensions {
			return ErrModel
		}
		if !validVector(v.Values, c) {
			return ErrInvalid
		}
	}
	return nil
}
func clone(d Document) Document {
	if d.Point != nil {
		p := *d.Point
		d.Point = &p
	}
	if d.Embedding != nil {
		v := *d.Embedding
		v.Values = append([]float64(nil), v.Values...)
		d.Embedding = &v
	}
	return d
}

type Hit struct {
	ID       string  `json:"id"`
	Version  uint64  `json:"version"`
	Score    float64 `json:"score,omitempty"`
	Distance float64 `json:"distance,omitempty"`
}
type Work struct {
	Documents  int   `json:"documents"`
	Candidates int   `json:"candidates"`
	Postings   int   `json:"postings_visited"`
	Nodes      int   `json:"nodes_visited"`
	Distances  int   `json:"distance_evaluations"`
	WallNanos  int64 `json:"wall_nanos"`
}

func distance(a, b []float64, metric Metric) float64 {
	if metric == Euclidean {
		s := 0.0
		for i, x := range a {
			d := x - b[i]
			s = math.Hypot(s, d)
		}
		return s
	}
	aa, bb := 0.0, 0.0
	for i, x := range a {
		aa = math.Hypot(aa, x)
		bb = math.Hypot(bb, b[i])
	}
	na, nb := aa, bb
	v := 0.0
	for i, x := range a {
		v += (x / na) * (b[i] / nb)
	}
	return 1 - math.Max(-1, math.Min(1, v))
}
