package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/resilience"
)

type Client struct {
	Base, Token string
	HTTP        *http.Client
	Policy      *resilience.Policy
	// The durable payment workflow owns mutation retries by default. An explicit
	// comparison can enable immediate retries against this provider's durable ID
	// fence. Every attempt still uses the identical serialized intent.
	RetryMutations bool
}
type callError struct {
	retry  bool
	failed bool
}

func (e *callError) Error() string { return api.ErrUnknown.Error() }
func (e *callError) Unwrap() error { return api.ErrUnknown }

func NewClient(base, token string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || len(token) < 32 {
		return nil, errors.New("invalid provider configuration")
	}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: 300 * time.Millisecond}).DialContext, TLSHandshakeTimeout: 300 * time.Millisecond, MaxConnsPerHost: 8, ResponseHeaderTimeout: 350 * time.Millisecond}
	policy, _ := resilience.NewPolicy(8, 16, 3, 1200*time.Millisecond, 400*time.Millisecond)
	policy.Classify = func(err error) resilience.Classification {
		if err == nil {
			return resilience.Classification{Outcome: resilience.Healthy}
		}
		var ce *callError
		if errors.As(err, &ce) {
			outcome := resilience.Healthy
			if ce.failed {
				outcome = resilience.Failed
			}
			return resilience.Classification{Retry: ce.retry, Outcome: outcome}
		}
		return resilience.Classification{Retry: true, Outcome: resilience.Failed}
	}
	return &Client{Base: strings.TrimRight(base, "/"), Token: token, HTTP: &http.Client{Transport: transport, Timeout: 1200 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("provider redirects refused") }}, Policy: policy}, nil
}
func (c *Client) call(ctx context.Context, i api.Intent, kind string) (api.Receipt, error) {
	body, err := json.Marshal(i)
	if err != nil {
		return api.Receipt{}, err
	}
	safety := resilience.NoRetry
	if kind == "lookup" {
		safety = resilience.ReadOnly
	} else if c.RetryMutations {
		safety = resilience.Idempotent
	}
	out, err := resilience.Do(ctx, c.Policy, safety, func(attempt context.Context) (api.Receipt, error) {
		request, err := http.NewRequestWithContext(attempt, "POST", c.Base+"/v1/payments/"+i.ID+"/"+kind, bytes.NewReader(body))
		if err != nil {
			return api.Receipt{}, &callError{}
		}
		request.Header.Set("Authorization", "Bearer "+c.Token)
		request.Header.Set("Content-Type", "application/json")
		response, err := c.HTTP.Do(request)
		if err != nil {
			return api.Receipt{}, &callError{retry: true, failed: true}
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			transient := response.StatusCode == 429 || response.StatusCode >= 500
			return api.Receipt{}, &callError{retry: transient, failed: transient}
		}
		var receipt api.Receipt
		decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
		if err = decoder.Decode(&receipt); err != nil {
			return api.Receipt{}, &callError{failed: true}
		}
		if decoder.Decode(new(any)) != io.EOF || receipt.ID != i.ID || receipt.AmountMinor != i.AmountMinor || receipt.Currency != i.Currency {
			return api.Receipt{}, &callError{failed: true}
		}
		switch receipt.Status {
		case "pending", "captured", "declined", "voided", "refunded":
		default:
			return api.Receipt{}, &callError{failed: true}
		}
		return receipt, nil
	})
	if err != nil {
		return api.Receipt{}, errors.Join(api.ErrUnknown, err)
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
