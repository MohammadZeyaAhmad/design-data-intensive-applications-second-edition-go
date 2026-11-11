package sparql

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProtocolAuthenticationDecodingAndBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "atlas" || pass != "test-password-only" {
			t.Error("missing basic auth")
		}
		if r.Header.Get("Content-Type") != "application/sparql-query" {
			t.Error("wrong query type")
		}
		w.Header().Set("Content-Type", "application/sparql-results+json")
		w.Write([]byte(`{"head":{"vars":["n"]},"results":{"bindings":[{"n":{"type":"uri","value":"urn:one"}}]}}`))
	}))
	defer server.Close()
	c, e := NewClient(server.URL+"/routes", "test-password-only")
	if e != nil {
		t.Fatal(e)
	}
	rows, e := c.Select(context.Background(), "SELECT ?n WHERE {}")
	if e != nil || len(rows) != 1 {
		t.Fatal(rows, e)
	}
	if term, e := rows[0]["n"].Term(); e != nil || term.Value != "urn:one" {
		t.Fatal(term, e)
	}
	if _, e = c.Select(context.Background(), strings.Repeat("x", (8<<20)+1)); e == nil {
		t.Fatal("unbounded request accepted")
	}
	for _, base := range []string{"http://example.com/routes", server.URL + "/routes?query=x", "http://a:b@127.0.0.1/routes", server.URL + "/other"} {
		if _, e := NewClient(base, "test-password-only"); e == nil {
			t.Fatal(base)
		}
	}
}

func TestRedirectAndMalformedResultsFail(t *testing.T) {
	for _, kind := range []string{"redirect", "malformed", "boolean"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if kind == "redirect" {
					http.Redirect(w, r, "http://example.com", 302)
					return
				}
				if kind == "boolean" {
					w.Write([]byte(`{"boolean":true}`))
					return
				}
				w.Write([]byte(`{"head":{"vars":["n"]},"results":{}}`))
			}))
			defer server.Close()
			c, e := NewClient(server.URL+"/routes", "test-password-only")
			if e != nil {
				t.Fatal(e)
			}
			if _, e = c.Select(context.Background(), "SELECT ?n WHERE{}"); e == nil {
				t.Fatal("invalid SELECT response accepted")
			}
		})
	}
}
