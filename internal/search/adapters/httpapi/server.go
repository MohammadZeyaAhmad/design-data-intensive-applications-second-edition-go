package httpapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/embed"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/ports"
)

func New(store ports.Store, keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/search/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			SKU             string        `json:"sku"`
			Warehouse       string        `json:"warehouse"`
			Title           string        `json:"title"`
			Body            string        `json:"body"`
			Point           *domain.Point `json:"point"`
			ExpectedVersion int64         `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		d := domain.Document{ID: r.PathValue("id"), SKU: b.SKU, Warehouse: b.Warehouse, Title: b.Title, Body: b.Body, Point: b.Point, Version: b.ExpectedVersion}
		out, replayed, e := (publish.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), d)
		if e != nil {
			failure(w, e)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("DELETE /v1/search/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			ExpectedVersion int64 `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		out, replayed, e := (publish.Handler{Store: store}).Delete(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), r.PathValue("id"), b.ExpectedVersion)
		if e != nil {
			failure(w, e)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("GET /v1/search/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		d, e := store.Document(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, d)
	})
	for _, path := range []string{"text", "suggest"} {
		path := path
		mux.HandleFunc("GET /v1/search/"+path, func(w http.ResponseWriter, r *http.Request) {
			k, e := limit(r)
			if e != nil {
				failure(w, e)
				return
			}
			query := r.URL.Query().Get("q")
			if path == "text" {
				hits, e := store.Text(r.Context(), auth.Tenant(r.Context()), query, k)
				if e != nil {
					failure(w, e)
					return
				}
				httpx.JSON(w, 200, hits)
			} else {
				hits, e := store.Suggest(r.Context(), auth.Tenant(r.Context()), query, k)
				if e != nil {
					failure(w, e)
					return
				}
				httpx.JSON(w, 200, hits)
			}
		})
	}
	mux.HandleFunc("GET /v1/search/box", func(w http.ResponseWriter, r *http.Request) {
		k, e := limit(r)
		if e != nil {
			failure(w, e)
			return
		}
		a, e := floats(r, "min_lon", "min_lat", "max_lon", "max_lat")
		if e != nil {
			failure(w, e)
			return
		}
		hits, e := store.Box(r.Context(), auth.Tenant(r.Context()), domain.Box{MinLon: a[0], MinLat: a[1], MaxLon: a[2], MaxLat: a[3]}, k)
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, hits)
	})
	mux.HandleFunc("GET /v1/search/near", func(w http.ResponseWriter, r *http.Request) {
		k, e := limit(r)
		if e != nil {
			failure(w, e)
			return
		}
		a, e := floats(r, "lon", "lat", "km")
		if e != nil {
			failure(w, e)
			return
		}
		hits, e := store.Radius(r.Context(), auth.Tenant(r.Context()), domain.Point{Lon: a[0], Lat: a[1]}, a[2], k)
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, hits)
	})
	mux.HandleFunc("PUT /v1/search/documents/{id}/embedding", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			DocumentVersion int64     `json:"document_version"`
			Model           string    `json:"model"`
			ModelVersion    string    `json:"model_version"`
			Dimensions      int       `json:"dimensions"`
			Values          []float64 `json:"values"`
			ExpectedVersion int64     `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		d := domain.Embedding{ID: r.PathValue("id"), DocumentVersion: b.DocumentVersion, Model: b.Model, ModelVersion: b.ModelVersion, Dimensions: b.Dimensions, Values: b.Values, Version: b.ExpectedVersion}
		out, replayed, e := (embed.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), d)
		if e != nil {
			failure(w, e)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("POST /v1/search/vector", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Model        string    `json:"model"`
			ModelVersion string    `json:"model_version"`
			Values       []float64 `json:"values"`
			Limit        int       `json:"limit"`
			Metric       string    `json:"metric"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		if b.Limit == 0 {
			b.Limit = 10
		}
		if b.Metric == "" {
			b.Metric = "cosine"
		}
		hits, e := store.Vector(r.Context(), auth.Tenant(r.Context()), b.Model, b.ModelVersion, b.Values, b.Limit, b.Metric)
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, hits)
	})
	return httpx.Guard(auth.Middleware(keys)(mux), nil)
}
func limit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 10, nil
	}
	n, e := strconv.Atoi(raw)
	if e != nil || n < 1 || n > 100 {
		return 0, domain.ErrInvalid
	}
	return n, nil
}
func floats(r *http.Request, names ...string) ([]float64, error) {
	var values []float64
	for _, name := range names {
		v, e := strconv.ParseFloat(r.URL.Query().Get(name), 64)
		if e != nil {
			return nil, domain.ErrInvalid
		}
		values = append(values, v)
	}
	return values, nil
}
func failure(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, domain.ErrInvalid) || errors.Is(e, commands.ErrInvalid):
		httpx.Problem(w, 400, "invalid_input")
	case errors.Is(e, domain.ErrConflict) || errors.Is(e, commands.ErrConflict):
		httpx.Problem(w, 409, "version_or_request_conflict")
	case errors.Is(e, domain.ErrNotFound):
		httpx.Problem(w, 404, "not_found")
	case errors.Is(e, domain.ErrVectorDisabled):
		httpx.Problem(w, 503, "vector_profile_required")
	default:
		httpx.Problem(w, 500, "search_failure")
	}
}
