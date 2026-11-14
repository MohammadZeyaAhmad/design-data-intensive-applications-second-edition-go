package readrouting

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTokenAuthorityTenantHistoryAndBounds(t *testing.T) {
	s, _ := NewSigner([]byte(strings.Repeat("k", 32)), 5*time.Minute)
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	v, _ := s.Decode("acme", "")
	p := Point{Shard: "inventory", Source: "ba66b71d-4e81-474f-9237-059a51792f15", Epoch: 2, Position: LSN(123), Sticky: "replica-a"}
	token, e := s.Advance("acme", v, p)
	original := token
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.Decode("acme", token)
	if e != nil || v.Points[0] != p {
		t.Fatal(v, e)
	}
	for _, bad := range []string{token + "x", strings.Repeat("x", MaxTokenBytes+1), "a.b.c"} {
		if _, e = s.Decode("acme", bad); !errors.Is(e, ErrToken) {
			t.Fatal(e)
		}
	}
	if _, e = s.Decode("other", token); !errors.Is(e, ErrToken) {
		t.Fatal(e)
	}
	p.Position--
	if _, e = s.Advance("acme", v, p); !errors.Is(e, ErrHistory) {
		t.Fatal("regressed fence", e)
	}
	p.Position++
	p.Epoch++
	if _, e = s.Advance("acme", v, p); !errors.Is(e, ErrHistory) {
		t.Fatal("joined histories", e)
	}
	for i := 0; i < MaxPoints; i++ {
		p.Shard = string(rune('a' + i))
		p.Epoch = 2
		token, e = s.Advance("acme", v, p)
		if i == MaxPoints-1 {
			if !errors.Is(e, ErrToken) {
				t.Fatal("unbounded vector", e)
			}
		} else {
			if e != nil {
				t.Fatal(e)
			}
			v, e = s.Decode("acme", token)
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	now = now.Add(6 * time.Minute)
	if _, e = s.Decode("acme", original); !errors.Is(e, ErrExpired) {
		t.Fatal("expired token accepted")
	}
}
func FuzzSessionToken(f *testing.F) {
	f.Add("0/0")
	f.Add("FFFFFFFF/FFFFFFFF")
	f.Add("a.b")
	s, _ := NewSigner([]byte(strings.Repeat("f", 32)), time.Minute)
	s.now = func() time.Time { return time.Unix(1000, 0) }
	seed, _ := json.Marshal(Vector{Version: 1, Subject: subject("tenant"), Expires: 1060, Points: []Point{{Shard: "inventory", Source: "ba66b71d-4e81-474f-9237-059a51792f15", Epoch: 1, Position: 1}}})
	f.Add(string(seed))
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		if l, e := ParseLSN(input); e == nil {
			round, e := ParseLSN(l.String())
			if e != nil || round != l {
				t.Fatal("LSN roundtrip")
			}
		}
		_, _ = s.Decode("tenant", input)
		// Also fuzz authenticated payload grammar, rather than stopping every
		// mutated input at the MAC boundary. Input size is bounded above.
		payload := base64.RawURLEncoding.EncodeToString([]byte(input))
		m := hmac.New(sha256.New, s.key)
		_, _ = m.Write([]byte(payload))
		signed := payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
		_, _ = s.Decode("tenant", signed)
	})
}
