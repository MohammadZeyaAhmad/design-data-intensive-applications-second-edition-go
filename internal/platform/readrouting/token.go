// Package readrouting gates PostgreSQL reads on authenticated session fences.
// A returned position bounds future reads; it is not a snapshot identifier.
package readrouting

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrToken       = errors.New("invalid session token")
	ErrExpired     = errors.New("expired session token")
	ErrHistory     = errors.New("session history changed")
	ErrPending     = errors.New("replica has not reached session fence")
	ErrUnavailable = errors.New("read authority unavailable")
	ErrContract    = errors.New("unsupported read contract")
)

const MaxTokenBytes = 4096
const MaxPoints = 8

type LSN uint64

func ParseLSN(s string) (LSN, error) {
	p := strings.Split(s, "/")
	if len(p) != 2 || len(p[0]) < 1 || len(p[0]) > 8 || len(p[1]) < 1 || len(p[1]) > 8 {
		return 0, ErrToken
	}
	a, e := strconv.ParseUint(p[0], 16, 32)
	if e != nil {
		return 0, ErrToken
	}
	b, e := strconv.ParseUint(p[1], 16, 32)
	if e != nil {
		return 0, ErrToken
	}
	return LSN(a<<32 | b), nil
}
func (l LSN) String() string               { return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xffffffff) }
func (l LSN) MarshalJSON() ([]byte, error) { return json.Marshal(l.String()) }
func (l *LSN) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) != nil {
		return ErrToken
	}
	v, err := ParseLSN(s)
	*l = v
	return err
}

type Point struct {
	Shard    string `json:"shard"`
	Source   string `json:"source"`
	Epoch    int64  `json:"epoch"`
	Position LSN    `json:"lsn"`
	Sticky   string `json:"sticky,omitempty"`
}
type Vector struct {
	Version int     `json:"v"`
	Subject string  `json:"sub"`
	Expires int64   `json:"exp"`
	Points  []Point `json:"points"`
}
type Signer struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

func NewSigner(key []byte, ttl time.Duration) (*Signer, error) {
	if len(key) < 32 || len(key) > 256 || ttl < time.Second || ttl > time.Hour {
		return nil, ErrContract
	}
	return &Signer{key: append([]byte(nil), key...), ttl: ttl, now: time.Now}, nil
}
func subject(tenant string) string {
	h := sha256.Sum256([]byte(tenant))
	return hex.EncodeToString(h[:])
}
func label(s string) bool {
	if len(s) < 1 || len(s) > 32 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validPoint(p Point) bool {
	if !label(p.Shard) || p.Epoch < 1 || len(p.Source) != 36 || p.Sticky != "" && !label(p.Sticky) {
		return false
	}
	for i, c := range p.Source {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func (s *Signer) Decode(tenant, token string) (Vector, error) {
	if token == "" {
		return Vector{Version: 1, Subject: subject(tenant)}, nil
	}
	if len(token) > MaxTokenBytes {
		return Vector{}, ErrToken
	}
	p := strings.Split(token, ".")
	if len(p) != 2 {
		return Vector{}, ErrToken
	}
	sig, e := base64.RawURLEncoding.DecodeString(p[1])
	if e != nil || len(sig) != sha256.Size {
		return Vector{}, ErrToken
	}
	m := hmac.New(sha256.New, s.key)
	_, _ = m.Write([]byte(p[0]))
	if !hmac.Equal(sig, m.Sum(nil)) {
		return Vector{}, ErrToken
	}
	b, e := base64.RawURLEncoding.DecodeString(p[0])
	if e != nil {
		return Vector{}, ErrToken
	}
	var v Vector
	if json.Unmarshal(b, &v) != nil || v.Version != 1 || v.Subject != subject(tenant) || len(v.Points) < 1 || len(v.Points) > MaxPoints {
		return Vector{}, ErrToken
	}
	if v.Expires <= s.now().Unix() {
		return Vector{}, ErrExpired
	}
	if v.Expires > s.now().Add(s.ttl+time.Second).Unix() {
		return Vector{}, ErrToken
	}
	last := ""
	for _, p := range v.Points {
		if !validPoint(p) || p.Shard <= last {
			return Vector{}, ErrToken
		}
		last = p.Shard
	}
	return v, nil
}
func (s *Signer) Advance(tenant string, v Vector, p Point) (string, error) {
	if v.Subject != subject(tenant) || !validPoint(p) {
		return "", ErrToken
	}
	v.Points = append([]Point(nil), v.Points...)
	found := false
	for i, old := range v.Points {
		if old.Shard == p.Shard {
			if old.Source != p.Source || old.Epoch != p.Epoch {
				return "", ErrHistory
			}
			if old.Position > p.Position {
				return "", ErrHistory
			}
			v.Points[i] = p
			found = true
			break
		}
	}
	if !found {
		v.Points = append(v.Points, p)
	}
	if len(v.Points) > MaxPoints {
		return "", ErrToken
	}
	sort.Slice(v.Points, func(i, j int) bool { return v.Points[i].Shard < v.Points[j].Shard })
	v.Version = 1
	v.Expires = s.now().Add(s.ttl).Unix()
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	payload := base64.RawURLEncoding.EncodeToString(b)
	m := hmac.New(sha256.New, s.key)
	_, _ = m.Write([]byte(payload))
	out := payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
	if len(out) > MaxTokenBytes {
		return "", ErrToken
	}
	return out, nil
}
