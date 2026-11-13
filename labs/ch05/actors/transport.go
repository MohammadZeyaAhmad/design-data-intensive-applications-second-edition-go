package actors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Sender exposes the same reference/message operations locally and remotely.
// A successful Tell reports admission only; Ask may time out after admission.
type Sender interface {
	Resolve(context.Context, Key) (Ref, error)
	Tell(context.Context, Ref, []byte) (Acceptance, error)
	Ask(context.Context, Ref, []byte) (Reply, error)
}
type Local struct{ Runtime *Runtime }

func (s Local) Resolve(ctx context.Context, key Key) (Ref, error) {
	if e := ctx.Err(); e != nil {
		return Ref{}, e
	}
	return s.Runtime.Resolve(key)
}
func (s Local) Tell(ctx context.Context, ref Ref, raw []byte) (Acceptance, error) {
	return s.Runtime.Tell(ctx, ref, raw)
}
func (s Local) Ask(ctx context.Context, ref Ref, raw []byte) (Reply, error) {
	return s.Runtime.Ask(ctx, ref, raw)
}

type Client struct {
	base, token string
	http        *http.Client
}

func NewClient(base, token string) (*Client, error) {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || u.User != nil || u.RawQuery != "" || u.Path != "" || len(token) < 32 || len(token) > 256 {
		return nil, ErrInvalid
	}
	return &Client{base: base, token: token, http: &http.Client{Timeout: 3 * time.Second}}, nil
}

type request struct {
	Ref     Ref             `json:"ref"`
	Message json.RawMessage `json:"message"`
}
type Diagnostics struct {
	Stats       Stats        `json:"stats"`
	DeadLetters []DeadLetter `json:"dead_letters"`
}

func (c *Client) call(ctx context.Context, path string, input, output any) error {
	var body []byte
	var e error
	method := "GET"
	if input != nil {
		body, e = json.Marshal(input)
		if e != nil {
			return e
		}
		method = "POST"
	}
	if len(body) > 8192 {
		return ErrInvalid
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	response, e := c.http.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return e
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 32769))
	if e != nil {
		return e
	}
	if len(raw) > 32768 {
		return ErrInvalid
	}
	if response.StatusCode >= 300 {
		var problem struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(raw, &problem) != nil {
			return ErrInvalid
		}
		return fromCode(problem.Code)
	}
	return json.Unmarshal(raw, output)
}
func (c *Client) Resolve(ctx context.Context, key Key) (out Ref, err error) {
	err = c.call(ctx, "/resolve", key, &out)
	return
}
func (c *Client) Tell(ctx context.Context, ref Ref, raw []byte) (out Acceptance, err error) {
	err = c.call(ctx, "/tell", request{Ref: ref, Message: raw}, &out)
	return
}
func (c *Client) Ask(ctx context.Context, ref Ref, raw []byte) (out Reply, err error) {
	err = c.call(ctx, "/ask", request{Ref: ref, Message: raw}, &out)
	return
}
func (c *Client) Diagnostics(ctx context.Context) (out Diagnostics, err error) {
	err = c.call(ctx, "/diagnostics", nil, &out)
	return
}
func (c *Client) Ready(ctx context.Context) bool {
	var out map[string]string
	return c.call(ctx, "/healthz", nil, &out) == nil && out["status"] == "ready"
}
func fromCode(code string) error {
	switch code {
	case "backpressure":
		return ErrBackpressure
	case "capacity":
		return ErrCapacity
	case "stale_reference":
		return ErrStale
	case "stopped":
		return ErrStopped
	case "crashed":
		return ErrCrashed
	case "restarted":
		return ErrRestarted
	case "identity_conflict":
		return ErrIdentityConflict
	case "insufficient_stock":
		return ErrInsufficient
	case "unsupported":
		return ErrUnsupported
	case "deadline":
		return context.DeadlineExceeded
	case "canceled":
		return context.Canceled
	default:
		return ErrInvalid
	}
}
func failure(w http.ResponseWriter, e error) {
	code, status := "invalid", 400
	switch {
	case errors.Is(e, ErrBackpressure):
		code, status = "backpressure", 429
	case errors.Is(e, ErrCapacity):
		code, status = "capacity", 503
	case errors.Is(e, ErrStale):
		code, status = "stale_reference", 409
	case errors.Is(e, ErrStopped):
		code, status = "stopped", 503
	case errors.Is(e, ErrCrashed):
		code, status = "crashed", 503
	case errors.Is(e, ErrRestarted):
		code, status = "restarted", 503
	case errors.Is(e, ErrIdentityConflict):
		code, status = "identity_conflict", 409
	case errors.Is(e, ErrInsufficient):
		code, status = "insufficient_stock", 409
	case errors.Is(e, ErrUnsupported):
		code = "unsupported"
	case errors.Is(e, context.DeadlineExceeded):
		code, status = "deadline", 504
	case errors.Is(e, context.Canceled):
		code, status = "canceled", 408
	}
	respond(w, status, map[string]string{"code": code})
}
func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func Handler(runtime *Runtime, tenant, token string) http.Handler {
	expected := sha256.Sum256([]byte(token))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ready"}) })
	decode := func(w http.ResponseWriter, r *http.Request, out any) bool {
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
			failure(w, ErrInvalid)
			return false
		}
		return true
	}
	mux.HandleFunc("POST /resolve", func(w http.ResponseWriter, r *http.Request) {
		var key Key
		if !decode(w, r, &key) {
			return
		}
		if key.Tenant != tenant {
			respond(w, 401, map[string]string{"code": "unauthorized"})
			return
		}
		out, e := runtime.Resolve(key)
		if e != nil {
			failure(w, e)
			return
		}
		respond(w, 200, out)
	})
	send := func(ask bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var in request
			if !decode(w, r, &in) {
				return
			}
			if in.Ref.Key.Tenant != tenant {
				respond(w, 401, map[string]string{"code": "unauthorized"})
				return
			}
			if ask {
				out, e := runtime.Ask(r.Context(), in.Ref, in.Message)
				if e != nil {
					failure(w, e)
					return
				}
				respond(w, 200, out)
			} else {
				out, e := runtime.Tell(r.Context(), in.Ref, in.Message)
				if e != nil {
					failure(w, e)
					return
				}
				respond(w, 202, out)
			}
		}
	}
	mux.HandleFunc("POST /tell", send(false))
	mux.HandleFunc("POST /ask", send(true))
	mux.HandleFunc("GET /diagnostics", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, Diagnostics{Stats: runtime.Stats(), DeadLetters: runtime.DeadLetters()})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			auth := r.Header.Values("Authorization")
			if len(auth) != 1 {
				respond(w, 401, map[string]string{"code": "unauthorized"})
				return
			}
			value, ok := strings.CutPrefix(auth[0], "Bearer ")
			actual := sha256.Sum256([]byte(value))
			if !ok || len(value) > 256 || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
				respond(w, 401, map[string]string{"code": "unauthorized"})
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
