package rdf

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Parse accepts bounded one-triple-per-line N-Triples, comments, scalar typed
// literals and language tags. Blank labels are scoped to this document.
func Parse(r io.Reader, scope string) (*Store, error) {
	limited := io.LimitReader(r, (8<<20)+1)
	data, e := io.ReadAll(limited)
	if e != nil {
		return nil, e
	}
	if len(data) > 8<<20 {
		return nil, ErrBudget
	}
	if !utf8.Valid(data) {
		return nil, ErrInvalid
	}
	s := New()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 65536)
	line := 0
	for scanner.Scan() {
		line++
		p := parser{source: scanner.Text(), scope: scope}
		p.space()
		if p.end() || p.source[p.pos] == '#' {
			continue
		}
		a, e := p.term()
		if e != nil {
			return nil, fmt.Errorf("RDF line %d: %w", line, e)
		}
		if !p.gap() {
			return nil, ErrInvalid
		}
		b, e := p.term()
		if e != nil {
			return nil, e
		}
		if !p.gap() {
			return nil, ErrInvalid
		}
		c, e := p.term()
		if e != nil {
			return nil, e
		}
		p.space()
		if p.end() || p.source[p.pos] != '.' {
			return nil, ErrInvalid
		}
		p.pos++
		p.space()
		if !p.end() && p.source[p.pos] != '#' {
			return nil, ErrInvalid
		}
		if _, e = s.Add(Triple{a, b, c}); e != nil {
			return nil, e
		}
	}
	if e = scanner.Err(); e != nil {
		return nil, ErrBudget
	}
	return s, nil
}

type parser struct {
	source, scope string
	pos           int
}

func (p *parser) end() bool { return p.pos == len(p.source) }
func (p *parser) space() {
	for !p.end() && (p.source[p.pos] == ' ' || p.source[p.pos] == '\t' || p.source[p.pos] == '\r') {
		p.pos++
	}
}
func (p *parser) gap() bool { start := p.pos; p.space(); return p.pos > start }
func (p *parser) term() (Term, error) {
	if p.end() {
		return Term{}, ErrInvalid
	}
	switch p.source[p.pos] {
	case '<':
		p.pos++
		start := p.pos
		for !p.end() && p.source[p.pos] != '>' {
			if p.source[p.pos] == '\\' {
				return Term{}, ErrInvalid
			}
			p.pos++
		}
		if p.end() {
			return Term{}, ErrInvalid
		}
		t := URI(p.source[start:p.pos])
		p.pos++
		if !t.Valid() {
			return Term{}, ErrInvalid
		}
		return t, nil
	case '_':
		if !strings.HasPrefix(p.source[p.pos:], "_:") {
			return Term{}, ErrInvalid
		}
		p.pos += 2
		start := p.pos
		for !p.end() && p.source[p.pos] != ' ' && p.source[p.pos] != '\t' && p.source[p.pos] != '.' {
			p.pos++
		}
		return ScopedBlank(p.scope, p.source[start:p.pos])
	case '"':
		start := p.pos
		p.pos++
		escaped := false
		for !p.end() {
			c := p.source[p.pos]
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
				value, e := decodeLiteral(p.source[start:p.pos])
				if e != nil {
					return Term{}, ErrInvalid
				}
				t := String(value)
				if !p.end() && p.source[p.pos] == '@' {
					p.pos++
					start = p.pos
					for !p.end() && ((p.source[p.pos] >= 'A' && p.source[p.pos] <= 'Z') || (p.source[p.pos] >= 'a' && p.source[p.pos] <= 'z') || (p.source[p.pos] >= '0' && p.source[p.pos] <= '9') || p.source[p.pos] == '-') {
						p.pos++
					}
					t = Lang(value, p.source[start:p.pos])
				} else if strings.HasPrefix(p.source[p.pos:], "^^") {
					p.pos += 2
					dt, e := p.term()
					if e != nil || dt.Kind != IRI {
						return Term{}, ErrInvalid
					}
					t = Typed(value, dt.Value)
				}
				if !t.Valid() {
					return Term{}, ErrInvalid
				}
				return t, nil
			}
		}
	}
	return Term{}, ErrInvalid
}

// decodeLiteral accepts RDF ECHAR/UCHAR, rejecting non-RDF escapes and invalid
// Unicode scalar values rather than silently replacing them.
func decodeLiteral(source string) (string, error) {
	var b strings.Builder
	for i := 1; i < len(source)-1; i++ {
		c := source[i]
		if c != '\\' {
			if c < 0x20 || c == '"' {
				return "", ErrInvalid
			}
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(source)-1 {
			return "", ErrInvalid
		}
		switch source[i] {
		case 't':
			b.WriteByte('\t')
		case 'b':
			b.WriteByte('\b')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 'f':
			b.WriteByte('\f')
		case '\\', '"', '\'':
			b.WriteByte(source[i])
		case 'u', 'U':
			n := 4
			if source[i] == 'U' {
				n = 8
			}
			if i+n >= len(source)-1 {
				return "", ErrInvalid
			}
			v, e := strconv.ParseUint(source[i+1:i+1+n], 16, 32)
			if e != nil || !utf8.ValidRune(rune(v)) {
				return "", ErrInvalid
			}
			b.WriteRune(rune(v))
			i += n
		default:
			return "", ErrInvalid
		}
	}
	return b.String(), nil
}

// Integer emits canonical lexical text. Typed preserves supplied lexical text;
// numeric value comparison belongs to the query language, not term equality.
func Integer(n int64) Term { return Typed(strconv.FormatInt(n, 10), XSD+"integer") }
func Boolean(v bool) Term  { return Typed(strconv.FormatBool(v), XSD+"boolean") }
