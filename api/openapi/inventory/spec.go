// Package inventory exposes the authored OpenAPI 3.1 reservation/stock subset.
package inventory

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.json
var specification []byte

func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(specification)
}
