package datalog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"
)

type token struct {
	kind, text string
	offset     int
}
type parser struct {
	source      string
	pos, tokens int
	look        token
	err         error
}

func Parse(source string) (Program, error) {
	p := parser{source: source}
	out := Program{Schema: map[string]int{}}
	if len(source) > 65536 || !utf8.ValidString(source) {
		return out, ErrBudget
	}
	p.next()
	for p.err == nil && p.look.kind != "eof" {
		head := p.atom()
		if p.err != nil {
			break
		}
		if p.look.kind == "." {
			f := Fact{Name: head.Name}
			for _, t := range head.Terms {
				if t.Variable != "" {
					p.err = ErrInvalid
					break
				}
				f.Values = append(f.Values, t.Value)
			}
			out.Facts = append(out.Facts, f)
			p.next()
		} else if p.look.kind == ":-" {
			p.next()
			r := Rule{Head: head}
			for {
				r.Body = append(r.Body, p.atom())
				if p.err != nil || p.look.kind != "," {
					break
				}
				p.next()
			}
			p.expect(".")
			out.Rules = append(out.Rules, r)
		} else {
			p.err = ErrUnsupported
		}
		if len(out.Facts) > 32768 || len(out.Rules) > 32 {
			p.err = ErrBudget
		}
	}
	if p.err != nil {
		return Program{}, fmt.Errorf("Datalog byte %d: %w", p.pos, p.err)
	}
	for _, f := range out.Facts {
		if e := validFact(f, out.Schema); e != nil {
			return Program{}, e
		}
	}
	for _, r := range out.Rules {
		for _, a := range append([]Atom{r.Head}, r.Body...) {
			if n, ok := out.Schema[a.Name]; ok && n != len(a.Terms) {
				return Program{}, ErrInvalid
			}
			out.Schema[a.Name] = len(a.Terms)
		}
	}
	if e := validateProgram(out); e != nil {
		return Program{}, e
	}
	return out, nil
}
func (p *parser) expect(kind string) {
	if p.err != nil {
		return
	}
	if p.look.kind != kind {
		p.err = ErrInvalid
		return
	}
	p.next()
}
func (p *parser) atom() Atom {
	a := Atom{}
	if p.err != nil {
		return a
	}
	if p.look.kind != "id" || p.look.text == "not" {
		p.err = ErrUnsupported
		return a
	}
	a.Name = p.look.text
	p.next()
	p.expect("(")
	for p.err == nil {
		t := Term{}
		switch p.look.kind {
		case "var":
			t.Variable = p.look.text
		case "string":
			var s string
			if json.Unmarshal([]byte(p.look.text), &s) != nil {
				p.err = ErrInvalid
			}
			t.Value = String(s)
		case "iri":
			t.Value = IRI(p.look.text)
		case "int":
			n, e := strconv.ParseInt(p.look.text, 10, 64)
			if e != nil {
				p.err = ErrInvalid
			}
			t.Value = Integer(n)
		case "id":
			if p.look.text == "true" {
				t.Value = Boolean(true)
			} else if p.look.text == "false" {
				t.Value = Boolean(false)
			} else {
				p.err = ErrUnsupported
			}
		default:
			p.err = ErrUnsupported
		}
		if p.err != nil {
			return a
		}
		a.Terms = append(a.Terms, t)
		if len(a.Terms) > MaxArity {
			p.err = ErrBudget
			return a
		}
		p.next()
		if p.look.kind != "," {
			break
		}
		p.next()
	}
	p.expect(")")
	return a
}
func (p *parser) next() {
	if p.err != nil {
		return
	}
	p.tokens++
	if p.tokens > 16384 {
		p.err = ErrBudget
		return
	}
	for p.pos < len(p.source) {
		c := p.source[p.pos]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			p.pos++
			continue
		}
		if c == '#' {
			for p.pos < len(p.source) && p.source[p.pos] != '\n' {
				p.pos++
			}
			continue
		}
		break
	}
	start := p.pos
	if start == len(p.source) {
		p.look = token{kind: "eof", offset: start}
		return
	}
	c := p.source[p.pos]
	p.pos++
	if c == '(' || c == ')' || c == ',' || c == '.' {
		p.look = token{kind: string(c), offset: start}
		return
	}
	if c == ':' && p.pos < len(p.source) && p.source[p.pos] == '-' {
		p.pos++
		p.look = token{kind: ":-", offset: start}
		return
	}
	if c == '"' {
		escaped := false
		for p.pos < len(p.source) {
			c = p.source[p.pos]
			p.pos++
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				p.look = token{kind: "string", text: p.source[start:p.pos], offset: start}
				return
			}
		}
		p.err = ErrInvalid
		return
	}
	if c == '<' {
		for p.pos < len(p.source) && p.source[p.pos] != '>' {
			p.pos++
		}
		if p.pos == len(p.source) {
			p.err = ErrInvalid
			return
		}
		p.look = token{kind: "iri", text: p.source[start+1 : p.pos], offset: start}
		p.pos++
		return
	}
	if c == '-' || c >= '0' && c <= '9' {
		for p.pos < len(p.source) && p.source[p.pos] >= '0' && p.source[p.pos] <= '9' {
			p.pos++
		}
		p.look = token{kind: "int", text: p.source[start:p.pos], offset: start}
		return
	}
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
		for p.pos < len(p.source) {
			c = p.source[p.pos]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				break
			}
			p.pos++
		}
		kind := "id"
		if p.source[start] >= 'A' && p.source[start] <= 'Z' {
			kind = "var"
		}
		p.look = token{kind: kind, text: p.source[start:p.pos], offset: start}
		return
	}
	p.err = ErrUnsupported
}
