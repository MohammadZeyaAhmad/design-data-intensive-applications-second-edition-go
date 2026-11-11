package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func verifyModelsLive(c *client) error {
	id, e := identity.NewID()
	if e != nil {
		return e
	}
	sku := "model-" + id
	brand := "maker-" + id
	brandPath := "/v1/catalog/brands/" + brand
	productPath := "/v1/catalog/products/" + sku
	if _, e = c.expect("PUT", brandPath, "acme", id+"-brand", map[string]any{"name": "Original maker", "expected_version": 0}, 200); e != nil {
		return e
	}
	if _, e = c.expect("PUT", productPath, "acme", id+"-price", map[string]any{"name": "Model demonstration", "unit_price_minor": 1200, "currency": "USD", "expected_version": 0}, 200); e != nil {
		return e
	}
	body := map[string]any{"brand_id": brand, "description": "Shared references and embedded attributes", "attributes": map[string]any{"wireless": true, "weight_grams": 120}, "variants": []map[string]string{{"code": "blue", "label": "Blue"}, {"code": "red", "label": "Red"}}, "categories": []string{"electronics", "devices"}, "expected_version": 0}
	for i := 0; i < 2; i++ {
		if _, e = c.expect("PUT", productPath+"/details", "acme", id+"-details", body, 200); e != nil {
			return e
		}
	}
	if _, e = c.expect("PUT", brandPath, "acme", id+"-rename", map[string]any{"name": "Renamed maker", "expected_version": 1}, 200); e != nil {
		return e
	}
	b, e := c.expect("GET", brandPath, "acme", "", nil, 200)
	if e != nil {
		return e
	}
	var renamed struct {
		Version int64 `json:"version"`
	}
	if json.Unmarshal(b, &renamed) != nil || renamed.Version != 2 {
		return errors.New("brand revision failed")
	}
	b, e = c.expect("GET", productPath+"/details", "acme", "", nil, 200)
	if e != nil {
		return e
	}
	var details struct {
		Brand    string `json:"brand_name"`
		Version  int64  `json:"version"`
		Variants []any  `json:"variants"`
	}
	if json.Unmarshal(b, &details) != nil || details.Brand != "Renamed maker" || details.Version != 1 || len(details.Variants) != 2 {
		return errors.New("current normalized brand reference missing")
	}
	if _, e = c.expect("GET", productPath+"/details", "globex", "", nil, 404); e != nil {
		return e
	}
	if _, e = c.expect("PUT", productPath+"/details", "acme", id+"-stale", body, 409); e != nil {
		return e
	}
	body["expected_version"] = 1
	body["variants"] = []any{}
	body["categories"] = []any{}
	if _, e = c.expect("PUT", productPath+"/details", "acme", id+"-replace", body, 200); e != nil {
		return e
	}
	b, e = c.expect("GET", productPath+"/details", "acme", "", nil, 200)
	if e != nil {
		return e
	}
	if json.Unmarshal(b, &details) != nil || details.Version != 2 || len(details.Variants) != 0 {
		return errors.New("replacement retained stale variants")
	}
	b, e = c.expect("GET", productPath, "acme", "", nil, 200)
	if e != nil {
		return e
	}
	var price struct {
		Version int64 `json:"version"`
		Amount  int64 `json:"unit_price_minor"`
	}
	if json.Unmarshal(b, &price) != nil || price.Version != 1 || price.Amount != 1200 {
		return errors.New("details changed price authority")
	}
	fmt.Println("Catalog model HTTP verification passed: normalized references, nested attributes, replay, replacement, tenant isolation and independent price revisions.")
	return nil
}
