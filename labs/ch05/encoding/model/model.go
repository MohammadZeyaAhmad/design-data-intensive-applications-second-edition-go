// Package model defines the finite synthetic order record used by encoding labs.
package model

import (
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"
)

const MaxWire = 64 << 10
const MaxLines = 100

var ErrInvalid = errors.New("invalid encoded snapshot")
var ErrBudget = errors.New("encoding resource bound exceeded")
var name = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var tenant = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Line struct {
	SKU      string `json:"sku" msgpack:"sku" xml:"sku"`
	Quantity int64  `json:"quantity" msgpack:"quantity" xml:"quantity"`
}
type Record struct {
	Tenant     string            `json:"tenant_id"`
	ID         string            `json:"order_id"`
	Name       string            `json:"name"`
	Lines      []Line            `json:"lines"`
	Version    int64             `json:"version,string"`
	Expedited  *bool             `json:"expedited,omitempty"`
	LegacyNote string            `json:"legacy_note,omitempty"`
	Currency   *string           `json:"currency,omitempty"`
	Discount   *int64            `json:"discount_minor,omitempty,string"`
	Labels     map[string]string `json:"labels,omitempty"`
}

func Text(s string, max int) bool {
	if len(s) > max || !utf8.ValidString(s) {
		return false
	}
	// The common domain must survive XML 1.0 as well as binary encodings.
	// encoding/xml otherwise substitutes some invalid characters with U+FFFD.
	for _, r := range s {
		if (r < 0x20 && r != '\t' && r != '\n' && r != '\r') || r == 0xfffe || r == 0xffff {
			return false
		}
	}
	return true
}
func Validate(r Record) error {
	if !tenant.MatchString(r.Tenant) || !uuid.MatchString(r.ID) || r.Name == "" || !Text(r.Name, 128) || r.Version < 1 || len(r.Lines) < 1 || len(r.Lines) > MaxLines || !Text(r.LegacyNote, 128) || len(r.Labels) > 8 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, l := range r.Lines {
		if !name.MatchString(l.SKU) || l.Quantity < 1 || l.Quantity > 1000000000 || seen[l.SKU] {
			return ErrInvalid
		}
		seen[l.SKU] = true
	}
	if r.Currency != nil && (*r.Currency != "USD" && *r.Currency != "EUR") {
		return ErrInvalid
	}
	if r.Discount != nil && (*r.Discount < 0 || *r.Discount > 1000000000000) {
		return ErrInvalid
	}
	for k, v := range r.Labels {
		if !name.MatchString(k) || !Text(v, 64) {
			return ErrInvalid
		}
	}
	return nil
}
func Fixture(lines, version int) Record {
	f := false
	zero := int64(0)
	currency := "EUR"
	r := Record{Tenant: "acme", ID: "00000000-0000-0000-0000-000000000001", Name: "Synthetic order – 数据", Version: 9007199254740993, Lines: []Line{}, Expedited: &f}
	for i := 0; i < lines; i++ {
		r.Lines = append(r.Lines, Line{fmt.Sprintf("sku-%03d", i), int64(i + 1)})
	}
	if version == 1 {
		r.LegacyNote = "retired field"
	} else {
		r.Currency = &currency
		r.Discount = &zero
		r.Labels = map[string]string{"channel": "lab", "region": "west"}
	}
	return r
}
func Known(r Record) Record {
	r.Currency = nil
	r.Discount = nil
	r.Labels = nil
	r.LegacyNote = ""
	return r
}

// Gob flattens pointers and omits zero values, so explicit wrappers carry the
// presence bits that this domain needs. Native pointer-zero loss is tested too.
type BoolState struct{ Set, Value bool }
type IntState struct {
	Set   bool
	Value int64
}
type StringState struct {
	Set   bool
	Value string
}
type GobV1 struct {
	Tenant, ID, Name string
	Lines            []Line
	Version          int64
	Expedited        BoolState
	LegacyNote       string
}
type GobV2 struct {
	Tenant, ID, Name string
	Lines            []Line
	Version          int64
	Expedited        BoolState
	Currency         StringState
	Discount         IntState
	Labels           map[string]string
}

func ToGob1(r Record) GobV1 {
	g := GobV1{Tenant: r.Tenant, ID: r.ID, Name: r.Name, Lines: r.Lines, Version: r.Version, LegacyNote: r.LegacyNote}
	if r.Expedited != nil {
		g.Expedited = BoolState{true, *r.Expedited}
	}
	return g
}
func ToGob2(r Record) GobV2 {
	g := GobV2{Tenant: r.Tenant, ID: r.ID, Name: r.Name, Lines: r.Lines, Version: r.Version, Labels: r.Labels}
	if r.Expedited != nil {
		g.Expedited = BoolState{true, *r.Expedited}
	}
	if r.Currency != nil {
		g.Currency = StringState{true, *r.Currency}
	}
	if r.Discount != nil {
		g.Discount = IntState{true, *r.Discount}
	}
	return g
}
func (g GobV1) Record() Record {
	r := Record{Tenant: g.Tenant, ID: g.ID, Name: g.Name, Lines: g.Lines, Version: g.Version, LegacyNote: g.LegacyNote}
	if g.Expedited.Set {
		r.Expedited = &g.Expedited.Value
	}
	return r
}
func (g GobV2) Record() Record {
	r := Record{Tenant: g.Tenant, ID: g.ID, Name: g.Name, Lines: g.Lines, Version: g.Version, Labels: g.Labels}
	if g.Expedited.Set {
		r.Expedited = &g.Expedited.Value
	}
	if g.Currency.Set {
		r.Currency = &g.Currency.Value
	}
	if g.Discount.Set {
		r.Discount = &g.Discount.Value
	}
	return r
}
