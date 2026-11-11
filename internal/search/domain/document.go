package domain

import (
	"errors"
	"math"
	"regexp"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid search document or query")
var ErrConflict = errors.New("search document or representation version conflict")
var ErrNotFound = errors.New("search document not found")
var ErrVectorDisabled = errors.New("pgvector profile is not enabled")
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
var modelPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }
func finite(x float64) bool  { return !math.IsNaN(x) && !math.IsInf(x, 0) }

type Point struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
}

func (p Point) Valid() bool {
	return finite(p.Lon) && finite(p.Lat) && p.Lon >= -180 && p.Lon <= 180 && p.Lat >= -90 && p.Lat <= 90
}

type Document struct {
	ID        string `json:"id"`
	SKU       string `json:"sku,omitempty"`
	Warehouse string `json:"warehouse,omitempty"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Point     *Point `json:"point,omitempty"`
	Version   int64  `json:"version"`
	Deleted   bool   `json:"deleted"`
}

func (d Document) Valid() bool {
	return ValidID(d.ID) && (d.SKU == "" || ValidID(d.SKU)) && (d.Warehouse == "" || ValidID(d.Warehouse)) && d.Version >= 0 && d.Version < 1e12 && !d.Deleted && len(d.Title) > 0 && len(d.Title) <= 256 && len(d.Body) <= 4096 && utf8.ValidString(d.Title) && utf8.ValidString(d.Body) && (d.Point == nil || d.Point.Valid())
}

type Embedding struct {
	ID              string    `json:"id"`
	DocumentVersion int64     `json:"document_version"`
	Model           string    `json:"model"`
	ModelVersion    string    `json:"model_version"`
	Dimensions      int       `json:"dimensions"`
	Values          []float64 `json:"values"`
	Version         int64     `json:"version"`
}

func (e Embedding) Valid() bool {
	if !ValidID(e.ID) || e.DocumentVersion < 1 || e.DocumentVersion > 1e12 || e.Version < 0 || e.Version >= 1e12 || !modelPattern.MatchString(e.Model) || !modelPattern.MatchString(e.ModelVersion) || e.Dimensions != 8 || len(e.Values) != 8 {
		return false
	}
	norm := 0.0
	for _, x := range e.Values {
		if !finite(x) || math.Abs(x) > 1e6 {
			return false
		}
		f := float64(float32(x))
		norm += f * f
	}
	return norm > 0
}
func ValidVectorQuery(model, version string, values []float64, k int, metric string) bool {
	return (Embedding{ID: "query", DocumentVersion: 1, Model: model, ModelVersion: version, Dimensions: 8, Values: values}).Valid() && k >= 1 && k <= 100 && (metric == "cosine" || metric == "euclidean")
}
func ValidText(q string, k int) bool {
	return len(q) > 0 && len(q) <= 256 && utf8.ValidString(q) && k >= 1 && k <= 100
}

type Box struct{ MinLon, MinLat, MaxLon, MaxLat float64 }

func (b Box) Valid() bool {
	return (Point{b.MinLon, b.MinLat}).Valid() && (Point{b.MaxLon, b.MaxLat}).Valid() && b.MinLat <= b.MaxLat
}

const EarthKM = 6371.0088

func RadiusBox(p Point, km float64) Box {
	r := km / EarthKM
	deg := 180 / math.Pi
	lo, hi := p.Lat-r*deg-1e-9, p.Lat+r*deg+1e-9
	if lo <= -90 || hi >= 90 || r >= math.Pi/2 {
		return Box{-180, math.Max(-90, lo), 180, math.Min(90, hi)}
	}
	x := math.Sin(r) / math.Cos(p.Lat/deg)
	if x >= 1 {
		return Box{-180, lo, 180, hi}
	}
	delta := math.Asin(x)*deg + 1e-9
	lon := func(x float64) float64 { return math.Mod(x+540, 360) - 180 }
	return Box{lon(p.Lon - delta), lo, lon(p.Lon + delta), hi}
}
func ValidRadius(p Point, km float64, k int) bool {
	return p.Valid() && finite(km) && km >= 0 && km <= math.Pi*EarthKM && k >= 1 && k <= 100
}
