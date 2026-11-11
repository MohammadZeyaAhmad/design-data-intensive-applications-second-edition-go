// Package rdf implements bounded RDF terms, set-valued triples and N-Triples
// interchange. IRIs are identifiers, not instructions to fetch remote content.
package rdf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	Vocab      = "https://atlas.example/vocab#"
	Type       = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	Label      = "http://www.w3.org/2000/01/rdf-schema#label"
	XSD        = "http://www.w3.org/2001/XMLSchema#"
	LangString = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
)

var ErrInvalid = errors.New("invalid RDF term or triple")
var ErrBudget = errors.New("RDF bound exceeded")
var local = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var blank = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,95}$`)
var language = regexp.MustCompile(`^[A-Za-z]+(?:-[A-Za-z0-9]+)*$`)

type Kind string

const (
	IRI     Kind = "iri"
	Blank   Kind = "blank"
	Literal Kind = "literal"
)

type Term struct {
	Kind     Kind   `json:"kind"`
	Value    string `json:"value"`
	Datatype string `json:"datatype,omitempty"`
	Language string `json:"language,omitempty"`
}

func ValidIRI(s string) bool {
	if len(s) == 0 || len(s) > 1024 || !utf8.ValidString(s) {
		return false
	}
	u, e := url.Parse(s)
	if e != nil || !u.IsAbs() {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) || strings.ContainsRune(`<>"{}|^`+"`\\", r) {
			return false
		}
	}
	return true
}
func URI(s string) Term             { return Term{Kind: IRI, Value: s} }
func String(s string) Term          { return Term{Kind: Literal, Value: s, Datatype: XSD + "string"} }
func Typed(s, datatype string) Term { return Term{Kind: Literal, Value: s, Datatype: datatype} }
func Lang(s, tag string) Term {
	return Term{Kind: Literal, Value: s, Datatype: LangString, Language: strings.ToLower(tag)}
}
func ScopedBlank(scope, label string) (Term, error) {
	if scope == "" || len(scope) > 256 || !utf8.ValidString(scope) || !blank.MatchString(label) {
		return Term{}, ErrInvalid
	}
	sum := sha256.Sum256([]byte(scope + "\x00" + label))
	return Term{Kind: Blank, Value: "b" + hex.EncodeToString(sum[:16])}, nil
}
func (t Term) Valid() bool {
	if !utf8.ValidString(t.Value) || len(t.Value) > 4096 || strings.ContainsRune(t.Value, 0) {
		return false
	}
	switch t.Kind {
	case IRI:
		return ValidIRI(t.Value) && t.Datatype == "" && t.Language == ""
	case Blank:
		return blank.MatchString(t.Value) && t.Datatype == "" && t.Language == ""
	case Literal:
		if !ValidIRI(t.Datatype) {
			return false
		}
		if t.Language != "" {
			return t.Datatype == LangString && language.MatchString(t.Language) && t.Language == strings.ToLower(t.Language)
		}
		return t.Datatype != LangString
	}
	return false
}
func (t Term) NT() (string, error) {
	if !t.Valid() {
		return "", ErrInvalid
	}
	switch t.Kind {
	case IRI:
		return "<" + t.Value + ">", nil
	case Blank:
		return "_:" + t.Value, nil
	case Literal:
		b, _ := json.Marshal(t.Value)
		if t.Language != "" {
			return string(b) + "@" + t.Language, nil
		}
		if t.Datatype == XSD+"string" {
			return string(b), nil
		}
		return string(b) + "^^<" + t.Datatype + ">", nil
	}
	return "", ErrInvalid
}

type Triple struct {
	Subject   Term `json:"subject"`
	Predicate Term `json:"predicate"`
	Object    Term `json:"object"`
}

func (t Triple) Valid() bool {
	return t.Subject.Valid() && t.Subject.Kind != Literal && t.Predicate.Valid() && t.Predicate.Kind == IRI && t.Object.Valid()
}
func (t Triple) NT() (string, error) {
	if !t.Valid() {
		return "", ErrInvalid
	}
	s, _ := t.Subject.NT()
	p, _ := t.Predicate.NT()
	o, _ := t.Object.NT()
	return s + " " + p + " " + o + " .", nil
}
func TenantURI(tenant string) (Term, error) {
	if !local.MatchString(tenant) {
		return Term{}, ErrInvalid
	}
	return URI("urn:atlas:tenant:" + tenant), nil
}
func EntityURI(tenant, kind, id string) (Term, error) {
	if !local.MatchString(tenant) || !local.MatchString(id) || (kind != "node" && kind != "edge") {
		return Term{}, ErrInvalid
	}
	return URI("urn:atlas:tenant:" + tenant + ":" + kind + ":" + id), nil
}
func NodeID(t Term, tenant string) (string, error) {
	prefix := "urn:atlas:tenant:" + tenant + ":node:"
	if t.Kind != IRI || !strings.HasPrefix(t.Value, prefix) {
		return "", ErrInvalid
	}
	id := strings.TrimPrefix(t.Value, prefix)
	if !local.MatchString(id) {
		return "", ErrInvalid
	}
	return id, nil
}
func (t Term) String() string {
	s, e := t.NT()
	if e != nil {
		return fmt.Sprintf("invalid(%s)", t.Kind)
	}
	return s
}
