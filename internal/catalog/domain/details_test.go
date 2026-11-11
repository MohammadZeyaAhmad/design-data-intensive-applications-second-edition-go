package domain

import (
	"encoding/json"
	"testing"
)

func TestDetailBoundsAndRelationships(t *testing.T) {
	d := Details{SKU: "sku", BrandID: "brand", Attributes: json.RawMessage(`{"weight_grams":120}`), Categories: []string{"devices"}, Variants: []Variant{{Code: "blue", Label: "Blue"}}}
	if !ValidDetails(d) {
		t.Fatal("valid details rejected")
	}
	for _, body := range []string{`null`, `[]`, `"text"`, `{"x":"\u0000"}`} {
		bad := d
		bad.Attributes = json.RawMessage(body)
		if ValidDetails(bad) {
			t.Fatal("invalid JSONB object accepted", body)
		}
	}
	bad := d
	bad.Categories = []string{"devices", "devices"}
	if ValidDetails(bad) {
		t.Fatal("duplicate membership accepted")
	}
	bad = d
	bad.Variants = append(bad.Variants, bad.Variants[0])
	if ValidDetails(bad) {
		t.Fatal("duplicate variant accepted")
	}
	if ValidBrand(Brand{ID: "brand", Name: "invalid\x00name"}) {
		t.Fatal("database-incompatible name accepted")
	}
}
