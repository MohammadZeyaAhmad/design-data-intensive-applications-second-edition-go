package readrouting

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SessionClient serializes operations within one session, retaining its latest
// server-signed watermark even when different goroutines share the client.
// Use separate clients for independent sessions. No server session map is used.
type SessionClient struct {
	base, bearer    string
	client          *http.Client
	gate            chan struct{}
	token           string
	primaryRequired bool
}

func NewSessionClient(base, bearer string, c *http.Client) (*SessionClient, error) {
	u, e := url.Parse(base)
	if e != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || bearer == "" {
		return nil, ErrContract
	}
	if c == nil {
		c = &http.Client{}
	}
	// Never forward a bearer/session token through a redirect to another host.
	copyClient := *c
	if copyClient.Timeout == 0 {
		copyClient.Timeout = 5 * time.Second
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &SessionClient{base: strings.TrimRight(base, "/"), bearer: bearer, client: &copyClient, gate: make(chan struct{}, 1), primaryRequired: true}, nil
}
func (c *SessionClient) Do(ctx context.Context, method, path string, body io.Reader, idempotencyKey ...string) (*http.Response, error) {
	if len(idempotencyKey) > 1 {
		return nil, ErrContract
	}
	u, e := url.Parse(path)
	if e != nil || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || u.IsAbs() || u.Host != "" || u.Fragment != "" {
		return nil, ErrContract
	}
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Bearer "+c.bearer)
	req.Header.Set("Content-Type", "application/json")
	if len(idempotencyKey) == 1 {
		req.Header.Set("Idempotency-Key", idempotencyKey[0])
	}
	if c.token != "" {
		req.Header.Set("X-Session-Token", c.token)
	}
	read := method == http.MethodGet
	if read {
		mode := "replica"
		if c.primaryRequired {
			mode = "primary"
		}
		req.Header.Set("X-Read-Consistency", mode)
		req.Header.Set("X-Read-On-Lag", "primary")
	}
	// A failed HTTP exchange can hide an already committed command.
	if !read {
		c.primaryRequired = true
	}
	resp, e := c.client.Do(req)
	if e != nil {
		return nil, e
	}
	if t := resp.Header.Get("X-Session-Token"); t != "" && len(t) <= MaxTokenBytes && len(resp.Header.Values("X-Session-Token")) == 1 && resp.Header.Get("X-Session-Status") == "issued" && (resp.StatusCode < 300 || read && resp.StatusCode == 404) {
		c.token = t
		c.primaryRequired = false
	}
	return resp, nil
}
