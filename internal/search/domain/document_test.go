package domain

import (
	"math"
	"testing"
)

func TestDocumentAndEmbeddingBounds(t *testing.T) {
	d := Document{ID: "sku-1", Title: "Wireless mouse", Point: &Point{179.9, 10}}
	if !d.Valid() {
		t.Fatal("valid document rejected")
	}
	d.Point.Lat = 91
	if d.Valid() {
		t.Fatal("latitude accepted")
	}
	v := Embedding{ID: "sku-1", DocumentVersion: 1, Model: "synthetic", ModelVersion: "v1.0", Dimensions: 8, Values: []float64{1, 0, 0, 0, 0, 0, 0, 0}}
	if !v.Valid() {
		t.Fatal("valid model metadata rejected")
	}
	v.Values[0] = math.NaN()
	if v.Valid() {
		t.Fatal("NaN accepted")
	}
	v.Values[0] = 1e-100
	if v.Valid() {
		t.Fatal("float32 zero cosine embedding accepted")
	}
	v.Values[0] = 1
	v.Dimensions = 7
	if v.Valid() {
		t.Fatal("dimension mismatch accepted")
	}
}
func TestRadiusBoxesCoverPolesAndWrap(t *testing.T) {
	if b := RadiusBox(Point{179.9, 0}, 50); b.MinLon <= b.MaxLon {
		t.Fatal("dateline not wrapped", b)
	}
	if b := RadiusBox(Point{0, 90}, 10); b.MinLon != -180 || b.MaxLon != 180 {
		t.Fatal("polar cap narrowed longitude", b)
	}
}
