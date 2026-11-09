package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	Base, Token string
	HTTP        *http.Client
}

func NewClient(base, token string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || len(token) < 32 {
		return nil, errors.New("invalid provider configuration")
	}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, TLSHandshakeTimeout: time.Second}
	return &Client{Base: strings.TrimRight(base, "/"), Token: token, HTTP: &http.Client{Transport: transport, Timeout: 1200 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("provider redirects refused") }}}, nil
}
func (c *Client) call(ctx context.Context, i api.Intent, kind string) (api.Receipt, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return api.Receipt{}, err
	}
	r, err := http.NewRequestWithContext(ctx, "POST", c.Base+"/v1/payments/"+i.ID+"/"+kind, bytes.NewReader(b))
	if err != nil {
		return api.Receipt{}, api.ErrUnknown
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("Content-Type", "application/json")
	response, err := c.HTTP.Do(r)
	if err != nil {
		return api.Receipt{}, api.ErrUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return api.Receipt{}, api.ErrUnknown
	}
	var out api.Receipt
	err = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&out)
	if err != nil {
		return api.Receipt{}, api.ErrUnknown
	}
	return out, nil
}
func (c *Client) Get(ctx context.Context, i api.Intent) (api.Receipt, error) {
	return c.call(ctx, i, "lookup")
}
func (c *Client) Capture(ctx context.Context, i api.Intent) (api.Receipt, error) {
	return c.call(ctx, i, "capture")
}
func (c *Client) Close(ctx context.Context, i api.Intent) (api.Receipt, error) {
	return c.call(ctx, i, "close")
}
