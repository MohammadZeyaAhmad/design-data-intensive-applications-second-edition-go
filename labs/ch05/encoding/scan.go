package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

func wireSize(b []byte) error {
	if len(b) == 0 {
		return model.ErrInvalid
	}
	if len(b) > model.MaxWire {
		return model.ErrBudget
	}
	return nil
}
func scanJSON(b []byte) (any, error) {
	if e := wireSize(b); e != nil {
		return nil, e
	}
	if !utf8.Valid(b) {
		return nil, model.ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	tokens := 0
	var value func(int) (any, error)
	value = func(depth int) (any, error) {
		tokens++
		if depth > 8 || tokens > 2048 {
			return nil, model.ErrBudget
		}
		t, e := d.Token()
		if e != nil {
			return nil, model.ErrInvalid
		}
		switch x := t.(type) {
		case json.Delim:
			switch x {
			case '{':
				out := map[string]any{}
				for d.More() {
					if len(out) >= 32 {
						return nil, model.ErrBudget
					}
					key, e := d.Token()
					if e != nil {
						return nil, model.ErrInvalid
					}
					s, ok := key.(string)
					if !ok || !model.Text(s, 64) {
						return nil, model.ErrInvalid
					}
					if _, ok = out[s]; ok {
						return nil, model.ErrInvalid
					}
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					out[s] = v
				}
				end, e := d.Token()
				if e != nil || end != json.Delim('}') {
					return nil, model.ErrInvalid
				}
				return out, nil
			case '[':
				out := []any{}
				for d.More() {
					if len(out) >= 100 {
						return nil, model.ErrBudget
					}
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					out = append(out, v)
				}
				end, e := d.Token()
				if e != nil || end != json.Delim(']') {
					return nil, model.ErrInvalid
				}
				return out, nil
			}
			return nil, model.ErrInvalid
		case string:
			if !model.Text(x, 256) {
				return nil, model.ErrBudget
			}
		case json.Number:
			if len(x) > 32 {
				return nil, model.ErrBudget
			}
			if _, e := strconv.ParseFloat(string(x), 64); e != nil {
				return nil, model.ErrInvalid
			}
		}
		return t, nil
	}
	out, e := value(0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, model.ErrInvalid
	}
	return out, nil
}
func scanXML(b []byte) error {
	if e := wireSize(b); e != nil {
		return e
	}
	if !utf8.Valid(b) {
		return model.ErrInvalid
	}
	d := xml.NewDecoder(bytes.NewReader(b))
	depth, tokens, roots, lines := 0, 0, 0, 0
	stack := []string{}
	children := []map[string]bool{}
	for {
		t, e := d.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return model.ErrInvalid
		}
		tokens++
		if tokens > 4096 {
			return model.ErrBudget
		}
		switch x := t.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
				if roots > 1 || x.Name.Local != "snapshot" {
					return model.ErrInvalid
				}
			}
			if depth > 0 {
				repeated := (stack[depth-1] == "lines" && x.Name.Local == "line") || (stack[depth-1] == "labels" && x.Name.Local == "label")
				if children[depth-1][x.Name.Local] && !repeated {
					return model.ErrInvalid
				}
				children[depth-1][x.Name.Local] = true
			}
			depth++
			if depth > 8 || x.Name.Space != "" || len(x.Attr) > 0 {
				return model.ErrInvalid
			}
			stack = append(stack, x.Name.Local)
			children = append(children, map[string]bool{})
			if x.Name.Local == "line" {
				lines++
				if lines > model.MaxLines {
					return model.ErrBudget
				}
			}
		case xml.EndElement:
			depth--
			if depth < 0 {
				return model.ErrInvalid
			}
			stack = stack[:len(stack)-1]
			children = children[:len(children)-1]
		case xml.CharData:
			if len(bytes.TrimSpace(x)) > 256 {
				return model.ErrBudget
			}
			if depth == 0 && len(bytes.TrimSpace(x)) > 0 {
				return model.ErrInvalid
			}
		case xml.Directive:
			return model.ErrInvalid
		case xml.ProcInst:
			if x.Target != "xml" || roots != 0 {
				return model.ErrInvalid
			}
		}
	}
	if roots != 1 || depth != 0 {
		return model.ErrInvalid
	}
	return nil
}

// MessagePack preflight rejects large declared containers before the generic
// library can allocate them. Only the record's map/string/integer/bool/nil
// profile is accepted; extension, binary and floating-point values are errors.
func scanMessagePack(b []byte) error {
	if e := wireSize(b); e != nil {
		return e
	}
	pos, nodes := 0, 0
	take := func(n int) ([]byte, error) {
		if n < 0 || n > len(b)-pos {
			return nil, model.ErrInvalid
		}
		v := b[pos : pos+n]
		pos += n
		return v, nil
	}
	length := func(n int) (int, error) {
		v, e := take(n)
		if e != nil {
			return 0, e
		}
		var k uint64
		switch n {
		case 1:
			k = uint64(v[0])
		case 2:
			k = uint64(binary.BigEndian.Uint16(v))
		case 4:
			k = uint64(binary.BigEndian.Uint32(v))
		}
		if k > uint64(model.MaxWire) {
			return 0, model.ErrBudget
		}
		return int(k), nil
	}
	var parse func(int, bool) (string, error)
	parse = func(depth int, key bool) (string, error) {
		nodes++
		if depth > 8 || nodes > 2048 {
			return "", model.ErrBudget
		}
		v, e := take(1)
		if e != nil {
			return "", e
		}
		c := v[0]
		stringsize, array, maplen := -1, -1, -1
		switch {
		case c >= 0xa0 && c <= 0xbf:
			stringsize = int(c & 31)
		case c >= 0x90 && c <= 0x9f:
			array = int(c & 15)
		case c >= 0x80 && c <= 0x8f:
			maplen = int(c & 15)
		case c == 0xd9 || c == 0xda || c == 0xdb:
			n := 1
			if c == 0xda {
				n = 2
			} else if c == 0xdb {
				n = 4
			}
			stringsize, e = length(n)
		case c == 0xdc || c == 0xdd:
			n := 2
			if c == 0xdd {
				n = 4
			}
			array, e = length(n)
		case c == 0xde || c == 0xdf:
			n := 2
			if c == 0xdf {
				n = 4
			}
			maplen, e = length(n)
		case c == 0xc0 || c == 0xc2 || c == 0xc3 || c <= 0x7f || c >= 0xe0:
		case c >= 0xcc && c <= 0xcf:
			_, e = take(1 << uint(c-0xcc))
		case c >= 0xd0 && c <= 0xd3:
			_, e = take(1 << uint(c-0xd0))
		default:
			return "", model.ErrInvalid
		}
		if e != nil {
			return "", e
		}
		if stringsize >= 0 {
			if stringsize > 256 {
				return "", model.ErrBudget
			}
			s, e := take(stringsize)
			if e != nil {
				return "", e
			}
			if !model.Text(string(s), 256) {
				return "", model.ErrInvalid
			}
			return string(s), nil
		}
		if key {
			return "", model.ErrInvalid
		}
		if array >= 0 {
			if array > 100 {
				return "", model.ErrBudget
			}
			for i := 0; i < array; i++ {
				if _, e = parse(depth+1, false); e != nil {
					return "", e
				}
			}
		}
		if maplen >= 0 {
			if maplen > 32 {
				return "", model.ErrBudget
			}
			seen := map[string]bool{}
			for i := 0; i < maplen; i++ {
				k, e := parse(depth+1, true)
				if e != nil {
					return "", e
				}
				if seen[k] {
					return "", model.ErrInvalid
				}
				seen[k] = true
				if _, e = parse(depth+1, false); e != nil {
					return "", e
				}
			}
		}
		return "", nil
	}
	if _, e := parse(0, false); e != nil {
		return e
	}
	if pos != len(b) {
		return model.ErrInvalid
	}
	return nil
}
