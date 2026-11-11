package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func verifySearchLive(c *client) error {
	nonce, e := identity.NewID()
	if e != nil {
		return e
	}
	id := "search-" + nonce
	term := "atlassearch" + strings.ReplaceAll(nonce, "-", "")
	sku := "searchsku-" + nonce
	if _, e = c.expect("PUT", "/v1/catalog/products/"+sku, "acme", id+"-catalog", map[string]any{"name": "Discovery demonstration", "unit_price_minor": 1000, "currency": "USD", "expected_version": 0}, 200); e != nil {
		return e
	}
	path := "/v1/search/documents/" + id
	body := map[string]any{"sku": sku, "warehouse": "east", "title": "Discovery " + term, "body": "Wireless pointing device", "point": map[string]float64{"lon": 179.9, "lat": 0}, "expected_version": 0}
	for i := 0; i < 2; i++ {
		b, e := c.expect("PUT", path, "acme", id+"-publish", body, 200)
		if e != nil {
			return e
		}
		var d struct {
			Version int64 `json:"version"`
		}
		if json.Unmarshal(b, &d) != nil || d.Version != 1 {
			return errors.New("document request replay advanced version")
		}
	}
	query := func(path string) ([]api.Hit, error) {
		b, e := c.expect("GET", path, "acme", "", nil, 200)
		if e != nil {
			return nil, e
		}
		var h []api.Hit
		e = json.Unmarshal(b, &h)
		return h, e
	}
	contains := func(hits []api.Hit, version int64) bool {
		for _, h := range hits {
			if h.ID == id && h.Version == version && h.SKU == sku {
				return true
			}
		}
		return false
	}
	hits, e := query("/v1/search/text?q=" + url.QueryEscape(term))
	if e != nil || !contains(hits, 1) {
		return errors.New("live text discovery missed published SKU reference")
	}
	hits, e = query("/v1/search/box?min_lon=170&min_lat=-1&max_lon=-170&max_lat=1&limit=100")
	if e != nil || !contains(hits, 1) {
		return errors.New("live wrapped rectangle missed document")
	}
	hits, e = query("/v1/search/near?lon=180&lat=0&km=20&limit=100")
	if e != nil || !contains(hits, 1) {
		return errors.New("live spherical radius missed document")
	}
	if _, e = c.expect("GET", path, "globex", "", nil, 404); e != nil {
		return e
	}
	modelVersion := "live-" + nonce
	vectorBody := map[string]any{"model": "atlas-synthetic", "model_version": modelVersion, "values": []float64{1, 0, 0, 0, 0, 0, 0, 0}, "limit": 10, "metric": "cosine"}
	vectorQuery := func() ([]api.Hit, error) {
		b, e := c.expect("POST", "/v1/search/vector", "acme", "", vectorBody, 200)
		if e != nil {
			return nil, e
		}
		var h []api.Hit
		e = json.Unmarshal(b, &h)
		return h, e
	}
	embedding := map[string]any{"document_version": 1, "model": "atlas-synthetic", "model_version": modelVersion, "dimensions": 8, "values": []float64{1, 0, 0, 0, 0, 0, 0, 0}, "expected_version": 0}
	enabled := os.Getenv("ATLAS_SEARCH_VECTOR_ENABLED") == "true"
	if enabled {
		if _, e = c.expect("PUT", path+"/embedding", "acme", id+"-embed", embedding, 200); e != nil {
			return e
		}
		hits, e = vectorQuery()
		if e != nil || !contains(hits, 1) {
			return errors.New("live vector discovery failed")
		}
	}
	body["expected_version"] = 1
	body["title"] = "Changed " + term + "new"
	body["point"] = map[string]float64{"lon": 0, "lat": 30}
	if _, e = c.expect("PUT", path, "acme", id+"-update", body, 200); e != nil {
		return e
	}
	hits, e = query("/v1/search/text?q=" + url.QueryEscape(term))
	if e != nil {
		return e
	}
	if contains(hits, 1) || contains(hits, 2) {
		return errors.New("stale text posting remained visible")
	}
	if enabled {
		hits, e = vectorQuery()
		if e != nil || len(hits) != 0 {
			return errors.New("stale document embedding remained searchable")
		}
		embedding["expected_version"] = 1
		if _, e = c.expect("PUT", path+"/embedding", "acme", id+"-stale-embed", embedding, 409); e != nil {
			return e
		}
		embedding["document_version"] = 2
		if _, e = c.expect("PUT", path+"/embedding", "acme", id+"-refresh-embed", embedding, 200); e != nil {
			return e
		}
		hits, e = vectorQuery()
		if e != nil || !contains(hits, 2) {
			return errors.New("fresh embedding missing")
		}
	}
	if _, e = c.expect("DELETE", path, "acme", id+"-delete", map[string]int64{"expected_version": 2}, 200); e != nil {
		return e
	}
	if _, e = c.expect("GET", path, "acme", "", nil, 404); e != nil {
		return e
	}
	if enabled {
		hits, e = vectorQuery()
		if e != nil || len(hits) != 0 {
			return errors.New("deleted vector document remained searchable")
		}
	}
	fmt.Println("Search HTTP verification passed: SKU reference, text, wrapped/spherical spatial query, tenant isolation, revisions, deletion; vector profile:", enabled)
	return nil
}
