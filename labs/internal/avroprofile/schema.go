// Package avroprofile bounds the finite, nonrecursive Avro profile used by the
// learning lab. The pinned runtime performs actual Avro parsing and resolution.
package avroprofile

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/twmb/avro"
)

const MaxSchema = 16 << 10
const MaxWire = 64 << 10
const MaxItems = 100

var ErrInvalid = errors.New("invalid Avro profile input")
var ErrBudget = errors.New("Avro profile resource bound exceeded")

type Field struct {
	Name string
	Type *Node
}
type Node struct {
	Kind     string
	Fields   []Field
	Items    *Node
	Branches []*Node
	Symbols  int
	Size     int
}

// Normalize preserves defaults, aliases and metadata. Parsing Canonical Form
// deliberately strips them and cannot identify the full semantic contract.
func Normalize(b []byte) ([]byte, error) {
	if len(b) == 0 || len(b) > MaxSchema || !utf8.Valid(b) {
		return nil, ErrBudget
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	tokens := 0
	var value func(int) (any, error)
	value = func(depth int) (any, error) {
		tokens++
		if depth > 16 || tokens > 2048 {
			return nil, ErrBudget
		}
		t, e := d.Token()
		if e != nil {
			return nil, ErrInvalid
		}
		switch x := t.(type) {
		case json.Delim:
			switch x {
			case '{':
				m := map[string]any{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return nil, ErrInvalid
					}
					key, ok := k.(string)
					if !ok || len(key) > 128 || len(m) >= 64 {
						return nil, ErrBudget
					}
					if _, exists := m[key]; exists {
						return nil, ErrInvalid
					}
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					m[key] = v
				}
				if t, e = d.Token(); e != nil || t != json.Delim('}') {
					return nil, ErrInvalid
				}
				return m, nil
			case '[':
				a := []any{}
				for d.More() {
					if len(a) >= 100 {
						return nil, ErrBudget
					}
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					a = append(a, v)
				}
				if t, e = d.Token(); e != nil || t != json.Delim(']') {
					return nil, ErrInvalid
				}
				return a, nil
			default:
				return nil, ErrInvalid
			}
		case string:
			if len(x) > 1024 {
				return nil, ErrBudget
			}
		case json.Number:
			if len(x) > 32 {
				return nil, ErrBudget
			}
			if i := strings.IndexAny(string(x), "eE"); i >= 0 {
				exponent, e := strconv.Atoi(string(x)[i+1:])
				if e != nil || exponent < -308 || exponent > 308 {
					return nil, ErrBudget
				}
			}
		}
		return t, nil
	}
	v, e := value(0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrInvalid
	}
	return json.Marshal(v)
}

func Compile(raw []byte) (*avro.Schema, *Node, []byte, error) {
	full, e := Normalize(raw)
	if e != nil {
		return nil, nil, nil, e
	}
	d := json.NewDecoder(bytes.NewReader(full))
	d.UseNumber()
	var source any
	if e = d.Decode(&source); e != nil {
		return nil, nil, nil, ErrInvalid
	}
	var shape func(any, int) error
	shape = func(value any, depth int) error {
		if depth > 8 {
			return ErrBudget
		}
		switch v := value.(type) {
		case string:
			return nil
		case []any:
			if len(v) < 1 || len(v) > 8 {
				return ErrBudget
			}
			for _, branch := range v {
				if e := shape(branch, depth+1); e != nil {
					return e
				}
			}
			return nil
		case map[string]any:
			if _, ok := v["logicalType"]; ok {
				return ErrBudget
			}
			kind, ok := v["type"].(string)
			if !ok {
				return ErrInvalid
			}
			switch kind {
			case "record":
				fields, ok := v["fields"].([]any)
				if !ok || len(fields) > 32 {
					return ErrBudget
				}
				for _, f := range fields {
					field, ok := f.(map[string]any)
					if !ok {
						return ErrInvalid
					}
					if _, ok = field["logicalType"]; ok {
						return ErrBudget
					}
					if e := shape(field["type"], depth+1); e != nil {
						return e
					}
				}
			case "array":
				return shape(v["items"], depth+1)
			case "map":
				return shape(v["values"], depth+1)
			case "enum":
				symbols, ok := v["symbols"].([]any)
				if !ok || len(symbols) > 32 {
					return ErrBudget
				}
			case "fixed":
				size, ok := v["size"].(json.Number)
				if !ok {
					return ErrInvalid
				}
				n, e := size.Int64()
				if e != nil || n < 1 || n > 256 {
					return ErrBudget
				}
			}
			return nil
		default:
			return ErrInvalid
		}
	}
	if e = shape(source, 0); e != nil {
		return nil, nil, nil, e
	}
	s, e := avro.Parse(string(full))
	if e != nil {
		return nil, nil, nil, ErrInvalid
	}
	n, e := Plan(s.Root())
	if e != nil {
		return nil, nil, nil, e
	}
	return s, n, full, nil
}

func Plan(root *avro.SchemaNode) (*Node, error) {
	named := map[string]*Node{}
	active := map[string]bool{}
	count := 0
	fields := 0
	var build func(*avro.SchemaNode, int) (*Node, error)
	build = func(s *avro.SchemaNode, depth int) (*Node, error) {
		count++
		if s == nil || depth > 8 || count > 256 || s.LogicalType != "" {
			return nil, ErrBudget
		}
		n := &Node{Kind: s.Type}
		full := s.Name
		if !strings.Contains(full, ".") && s.Namespace != "" {
			full = s.Namespace + "." + full
		}
		switch s.Type {
		case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		case "record":
			if active[full] || len(s.Fields) > 32 {
				return nil, ErrBudget
			}
			active[full] = true
			for _, f := range s.Fields {
				fields++
				if fields > 64 {
					return nil, ErrBudget
				}
				child, e := build(&f.Type, depth+1)
				if e != nil {
					return nil, e
				}
				n.Fields = append(n.Fields, Field{f.Name, child})
			}
			delete(active, full)
			named[full] = n
		case "enum":
			if len(s.Symbols) > 32 {
				return nil, ErrBudget
			}
			n.Symbols = len(s.Symbols)
			named[full] = n
		case "fixed":
			if s.Size < 1 || s.Size > 256 {
				return nil, ErrBudget
			}
			n.Size = s.Size
			named[full] = n
		case "array", "map":
			item := s.Items
			if s.Type == "map" {
				item = s.Values
			}
			var e error
			n.Items, e = build(item, depth+1)
			if e != nil {
				return nil, e
			}
		case "union":
			if len(s.Branches) < 1 || len(s.Branches) > 8 {
				return nil, ErrBudget
			}
			for i := range s.Branches {
				b, e := build(&s.Branches[i], depth+1)
				if e != nil {
					return nil, e
				}
				n.Branches = append(n.Branches, b)
			}
		default:
			if active[s.Type] {
				return nil, ErrBudget
			}
			ref := named[s.Type]
			if ref == nil {
				return nil, ErrInvalid
			}
			return ref, nil
		}
		return n, nil
	}
	return build(root, 0)
}
