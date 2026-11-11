package domain

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

type Brand struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int64  `json:"version"`
}
type Variant struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}
type Details struct {
	SKU         string          `json:"sku"`
	BrandID     string          `json:"brand_id"`
	BrandName   string          `json:"brand_name,omitempty"`
	Description string          `json:"description"`
	Attributes  json.RawMessage `json:"attributes"`
	Variants    []Variant       `json:"variants"`
	Categories  []string        `json:"categories"`
	Version     int64           `json:"version"`
}

func textValid(s string, max int) bool {
	return len(s) <= max && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func ValidBrand(b Brand) bool {
	return sku.MatchString(b.ID) && len(b.Name) > 0 && textValid(b.Name, 200) && b.Version >= 0 && b.Version < 1e12
}
func ValidDetails(d Details) bool {
	if !sku.MatchString(d.SKU) || !sku.MatchString(d.BrandID) || !textValid(d.Description, 4096) || d.Version < 0 || d.Version >= 1e12 || len(d.Variants) > 32 || len(d.Categories) > 16 || len(d.Attributes) > 4096 || !utf8.Valid(d.Attributes) {
		return false
	}
	var attributes map[string]json.RawMessage
	if len(d.Attributes) == 0 || json.Unmarshal(d.Attributes, &attributes) != nil || attributes == nil || len(attributes) > 32 || bytes.Contains(d.Attributes, []byte(`\u0000`)) {
		return false
	}
	for k := range attributes {
		if len(k) < 1 || !textValid(k, 64) {
			return false
		}
	}
	seen := map[string]bool{}
	for _, v := range d.Variants {
		if !sku.MatchString(v.Code) || len(v.Label) < 1 || !textValid(v.Label, 100) || seen[v.Code] {
			return false
		}
		seen[v.Code] = true
	}
	seen = map[string]bool{}
	for _, c := range d.Categories {
		if !sku.MatchString(c) || seen[c] {
			return false
		}
		seen[c] = true
	}
	return true
}
