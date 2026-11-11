package sparql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	base, password string
	http           *http.Client
}

func NewClient(base, password string) (*Client, error) {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" || u.Path != "/routes" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || len(password) < 12 {
		return nil, errors.New("RDF endpoint must be the authenticated loopback /routes dataset")
	}
	return &Client{base: base, password: password, http: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) post(ctx context.Context, endpoint, kind, source string) ([]byte, error) {
	if len(source) > 8<<20 {
		return nil, rdf.ErrBudget
	}
	request, e := http.NewRequestWithContext(ctx, "POST", c.base+endpoint, bytes.NewBufferString(source))
	if e != nil {
		return nil, e
	}
	request.Header.Set("Content-Type", kind)
	request.Header.Set("Accept", "application/sparql-results+json")
	request.SetBasicAuth("atlas", c.password)
	response, e := c.http.Do(request)
	if e != nil {
		return nil, e
	}
	defer response.Body.Close()
	body, e := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(body) > 2<<20 {
		return nil, rdf.ErrBudget
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("SPARQL endpoint status %d: %.256s", response.StatusCode, body)
	}
	return body, nil
}
func (c *Client) Update(ctx context.Context, source string) error {
	_, e := c.post(ctx, "/update", "application/sparql-update", source)
	return e
}

// LoadTurtle uses the Graph Store HTTP protocol. The caller owns the named
// graph and is responsible for its cleanup. Each request is a new document.
func (c *Client) LoadTurtle(ctx context.Context, graph rdf.Term, source string) error {
	if graph.Kind != rdf.IRI || !graph.Valid() {
		return rdf.ErrInvalid
	}
	_, e := c.post(ctx, "/data?graph="+url.QueryEscape(graph.Value), "text/turtle", source)
	return e
}

type Binding struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	Datatype string `json:"datatype"`
	Language string `json:"xml:lang"`
}

func (b Binding) Term() (rdf.Term, error) {
	var t rdf.Term
	switch b.Type {
	case "uri":
		t = rdf.URI(b.Value)
	case "bnode":
		t = rdf.Term{Kind: rdf.Blank, Value: b.Value}
	case "literal", "typed-literal":
		if b.Language != "" {
			t = rdf.Lang(b.Value, b.Language)
		} else if b.Datatype != "" {
			t = rdf.Typed(b.Value, b.Datatype)
		} else {
			t = rdf.String(b.Value)
		}
	default:
		return t, rdf.ErrInvalid
	}
	if !t.Valid() {
		return t, rdf.ErrInvalid
	}
	return t, nil
}
func (c *Client) Select(ctx context.Context, source string) ([]map[string]Binding, error) {
	body, e := c.post(ctx, "/query", "application/sparql-query", source)
	if e != nil {
		return nil, e
	}
	var result struct {
		Head struct {
			Vars []string `json:"vars"`
		} `json:"head"`
		Results struct {
			Bindings []map[string]Binding `json:"bindings"`
		} `json:"results"`
	}
	if e = json.Unmarshal(body, &result); e != nil || len(result.Head.Vars) == 0 || result.Results.Bindings == nil {
		return nil, rdf.ErrInvalid
	}
	if len(result.Results.Bindings) > 32768 {
		return nil, rdf.ErrBudget
	}
	return result.Results.Bindings, nil
}
func (c *Client) Ask(ctx context.Context, source string) (bool, error) {
	body, e := c.post(ctx, "/query", "application/sparql-query", source)
	if e != nil {
		return false, e
	}
	var result struct {
		Boolean *bool `json:"boolean"`
	}
	if json.Unmarshal(body, &result) != nil || result.Boolean == nil {
		return false, rdf.ErrInvalid
	}
	return *result.Boolean, nil
}
func (c *Client) Base() string { return c.base }
