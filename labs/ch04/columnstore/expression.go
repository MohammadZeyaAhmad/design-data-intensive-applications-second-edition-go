package columnstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type truth byte

const (
	falseValue truth = iota
	trueValue
	unknownValue
)

func and(a, b truth) truth {
	if a == falseValue || b == falseValue {
		return falseValue
	}
	if a == unknownValue || b == unknownValue {
		return unknownValue
	}
	return trueValue
}
func or(a, b truth) truth {
	if a == trueValue || b == trueValue {
		return trueValue
	}
	if a == unknownValue || b == unknownValue {
		return unknownValue
	}
	return falseValue
}
func not(a truth) truth {
	if a == trueValue {
		return falseValue
	}
	if a == falseValue {
		return trueValue
	}
	return unknownValue
}
func cmp(a, b Value, op string) truth {
	if !a.Valid || !b.Valid {
		return unknownValue
	}
	n := compare(a, b)
	ok := false
	switch op {
	case "=":
		ok = n == 0
	case "!=":
		ok = n != 0
	case "<":
		ok = n < 0
	case "<=":
		ok = n <= 0
	case ">":
		ok = n > 0
	case ">=":
		ok = n >= 0
	}
	if ok {
		return trueValue
	}
	return falseValue
}

type node struct {
	Op          string `json:"op"`
	Column      Column `json:"column,omitempty"`
	Literal     Value  `json:"literal"`
	Left, Right *node
}
type Expression struct {
	root   *node
	source string
	key    string
}

func (e *Expression) String() string {
	if e == nil {
		return ""
	}
	return e.source
}
func (e *Expression) Key() string {
	if e == nil {
		return ""
	}
	return e.key
}
func (e *Expression) refs() []Column {
	seen := map[Column]bool{}
	var walk func(*node)
	walk = func(n *node) {
		if n == nil {
			return
		}
		if n.Column != "" {
			seen[n.Column] = true
		}
		walk(n.Left)
		walk(n.Right)
	}
	walk(e.root)
	var r []Column
	for _, c := range columns {
		if seen[c] {
			r = append(r, c)
		}
	}
	return r
}
func eval(n *node, get func(Column) Value) truth {
	switch n.Op {
	case "true":
		return trueValue
	case "false":
		return falseValue
	case "and":
		return and(eval(n.Left, get), eval(n.Right, get))
	case "or":
		return or(eval(n.Left, get), eval(n.Right, get))
	case "not":
		return not(eval(n.Left, get))
	case "isnull":
		if !get(n.Column).Valid {
			return trueValue
		}
		return falseValue
	case "notnull":
		if get(n.Column).Valid {
			return trueValue
		}
		return falseValue
	default:
		return cmp(get(n.Column), n.Literal, n.Op)
	}
}

type token struct{ kind, value string }

func lex(s string) ([]token, error) {
	if len(s) < 1 || len(s) > 1024 || !utf8.ValidString(s) {
		return nil, fmt.Errorf("invalid filter size or text")
	}
	var ts []token
	for i := 0; i < len(s); {
		c := s[i]
		if strings.ContainsRune(" \t\r\n", rune(c)) {
			i++
			continue
		}
		t := token{}
		switch {
		case c == '\'':
			i++
			var b strings.Builder
			closed := false
			for i < len(s) {
				if s[i] == '\'' {
					i++
					if i < len(s) && s[i] == '\'' {
						b.WriteByte('\'')
						i++
						continue
					}
					closed = true
					break
				}
				b.WriteByte(s[i])
				i++
			}
			if !closed || b.Len() > 256 {
				return nil, fmt.Errorf("invalid string literal")
			}
			t = token{"string", b.String()}
		case c >= '0' && c <= '9' || c == '-':
			start := i
			i++
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
			t = token{"number", s[start:i]}
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
			start := i
			i++
			for i < len(s) && (s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] == '_' || s[i] >= '0' && s[i] <= '9') {
				i++
			}
			t = token{"word", strings.ToLower(s[start:i])}
		case c == '(' || c == ')':
			t = token{string(c), string(c)}
			i++
		case c == '=' || c == '!' || c == '<' || c == '>':
			op := string(c)
			i++
			if i < len(s) && s[i] == '=' && c != '=' {
				op += "="
				i++
			}
			if op == "!" {
				return nil, fmt.Errorf("invalid operator")
			}
			t = token{"op", op}
		default:
			return nil, fmt.Errorf("unsupported filter token")
		}
		ts = append(ts, t)
		if len(ts) > 256 {
			return nil, fmt.Errorf("too many filter tokens")
		}
	}
	return append(ts, token{"end", ""}), nil
}

type parser struct {
	tokens   []token
	i, depth int
}

func (p *parser) take(value string) bool {
	t := p.tokens[p.i]
	if t.kind != "string" && t.value == value {
		p.i++
		return true
	}
	return false
}
func (p *parser) expression() (*node, error) {
	a, e := p.conjunction()
	if e != nil {
		return nil, e
	}
	for p.take("or") {
		b, e := p.conjunction()
		if e != nil {
			return nil, e
		}
		a = &node{Op: "or", Left: a, Right: b}
	}
	return a, nil
}
func (p *parser) conjunction() (*node, error) {
	a, e := p.atom()
	if e != nil {
		return nil, e
	}
	for p.take("and") {
		b, e := p.atom()
		if e != nil {
			return nil, e
		}
		a = &node{Op: "and", Left: a, Right: b}
	}
	return a, nil
}
func (p *parser) atom() (*node, error) {
	p.depth++
	defer func() { p.depth-- }()
	if p.depth > 16 {
		return nil, fmt.Errorf("filter nesting limit")
	}
	if p.take("not") {
		a, e := p.atom()
		return &node{Op: "not", Left: a}, e
	}
	if p.take("(") {
		a, e := p.expression()
		if e != nil {
			return nil, e
		}
		if !p.take(")") {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		return a, nil
	}
	if p.take("true") {
		return &node{Op: "true"}, nil
	}
	if p.take("false") {
		return &node{Op: "false"}, nil
	}
	t := p.tokens[p.i]
	if t.kind != "word" || kind(Column(t.value)) == "" {
		return nil, fmt.Errorf("expected schema column")
	}
	c := Column(t.value)
	p.i++
	if p.take("is") {
		op := "isnull"
		if p.take("not") {
			op = "notnull"
		}
		if !p.take("null") {
			return nil, fmt.Errorf("expected NULL")
		}
		return &node{Op: op, Column: c}, nil
	}
	op := p.tokens[p.i]
	if op.kind != "op" {
		return nil, fmt.Errorf("expected comparison")
	}
	p.i++
	v := p.tokens[p.i]
	lit := null(kind(c))
	if v.kind == "word" && v.value == "null" {
	} else if v.kind == "string" && kind(c) == "string" {
		lit = text(v.value)
	} else if v.kind == "number" && kind(c) == "int" {
		n, e := strconv.ParseInt(v.value, 10, 64)
		if e != nil {
			return nil, fmt.Errorf("integer literal overflow")
		}
		lit = number(n)
	} else {
		return nil, fmt.Errorf("literal type does not match column")
	}
	p.i++
	return &node{Op: op.value, Column: c, Literal: lit}, nil
}
func Parse(s string) (*Expression, error) {
	ts, e := lex(s)
	if e != nil {
		return nil, e
	}
	p := parser{tokens: ts}
	root, e := p.expression()
	if e != nil {
		return nil, e
	}
	if p.tokens[p.i].kind != "end" {
		return nil, fmt.Errorf("unexpected filter suffix")
	}
	count := 0
	var valid func(*node, int) bool
	valid = func(n *node, d int) bool {
		if n == nil {
			return true
		}
		count++
		return d <= 16 && count <= 64 && valid(n.Left, d+1) && valid(n.Right, d+1)
	}
	if !valid(root, 1) {
		return nil, fmt.Errorf("filter node/depth limit")
	}
	b, _ := json.Marshal(root)
	h := sha256.Sum256(b)
	return &Expression{root: root, source: s, key: hex.EncodeToString(h[:])}, nil
}

type bitTruth struct{ T, U uint64 }
type vector struct {
	Values []Value
	Valid  uint64
	Equal  map[string]uint64
}

func evalBatch(n *node, cols map[Column]vector, all uint64) bitTruth {
	switch n.Op {
	case "true":
		return bitTruth{T: all}
	case "false":
		return bitTruth{}
	case "and":
		a, b := evalBatch(n.Left, cols, all), evalBatch(n.Right, cols, all)
		t := a.T & b.T
		return bitTruth{t, (a.T | a.U) & (b.T | b.U) &^ t}
	case "or":
		a, b := evalBatch(n.Left, cols, all), evalBatch(n.Right, cols, all)
		t := a.T | b.T
		return bitTruth{t, (a.U | b.U) &^ t}
	case "not":
		a := evalBatch(n.Left, cols, all)
		return bitTruth{all &^ (a.T | a.U), a.U}
	}
	v := cols[n.Column]
	if n.Op == "isnull" {
		return bitTruth{T: all &^ v.Valid}
	}
	if n.Op == "notnull" {
		return bitTruth{T: v.Valid}
	}
	if !n.Literal.Valid {
		return bitTruth{U: all}
	}
	if kind(n.Column) == "string" && (n.Op == "=" || n.Op == "!=") {
		t := v.Equal[n.Literal.S]
		if n.Op == "!=" {
			t = v.Valid &^ t
		}
		return bitTruth{t, all &^ v.Valid}
	}
	r := bitTruth{U: all &^ v.Valid}
	for i, value := range v.Values {
		if cmp(value, n.Literal, n.Op) == trueValue {
			r.T |= uint64(1) << i
		}
	}
	return r
}
