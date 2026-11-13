package avroprofile

import (
	"encoding/binary"
	"math"
	"unicode/utf8"
)

type Cursor struct {
	Data   []byte
	Pos    int
	Values int
}

func (c *Cursor) Take(n int) ([]byte, error) {
	if n < 0 || n > len(c.Data)-c.Pos {
		return nil, ErrInvalid
	}
	v := c.Data[c.Pos : c.Pos+n]
	c.Pos += n
	return v, nil
}
func (c *Cursor) Long() (int64, error) {
	u, n := binary.Uvarint(c.Data[c.Pos:])
	if n <= 0 {
		return 0, ErrInvalid
	}
	c.Pos += n
	return int64(u>>1) ^ -int64(u&1), nil
}
func (c *Cursor) Bytes(max int) ([]byte, error) {
	n, e := c.Long()
	if e != nil || n < 0 || n > int64(max) {
		return nil, ErrBudget
	}
	return c.Take(int(n))
}
func (c *Cursor) String(max int) (string, error) {
	b, e := c.Bytes(max)
	if e != nil {
		return "", e
	}
	if !utf8.Valid(b) {
		return "", ErrInvalid
	}
	return string(b), nil
}

func (c *Cursor) Value(n *Node, depth int) error {
	c.Values++
	if n == nil || depth > 8 || c.Values > 2048 {
		return ErrBudget
	}
	switch n.Kind {
	case "null":
		return nil
	case "boolean":
		b, e := c.Take(1)
		if e != nil || b[0] > 1 {
			return ErrInvalid
		}
	case "int", "long", "enum":
		v, e := c.Long()
		if e != nil {
			return e
		}
		if n.Kind == "int" && (v < math.MinInt32 || v > math.MaxInt32) {
			return ErrInvalid
		}
		if n.Kind == "enum" && (v < 0 || v >= int64(n.Symbols)) {
			return ErrInvalid
		}
	case "string":
		_, e := c.String(256)
		return e
	case "bytes":
		_, e := c.Bytes(256)
		return e
	case "fixed":
		_, e := c.Take(n.Size)
		return e
	case "float":
		b, e := c.Take(4)
		if e != nil {
			return e
		}
		f := float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ErrInvalid
		}
	case "double":
		b, e := c.Take(8)
		if e != nil {
			return e
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(b))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ErrInvalid
		}
	case "record":
		for _, f := range n.Fields {
			if e := c.Value(f.Type, depth+1); e != nil {
				return e
			}
		}
	case "union":
		branch, e := c.Long()
		if e != nil || branch < 0 || branch >= int64(len(n.Branches)) {
			return ErrInvalid
		}
		return c.Value(n.Branches[branch], depth+1)
	case "array", "map":
		total := int64(0)
		keys := map[string]bool{}
		for {
			count, e := c.Long()
			if e != nil {
				return e
			}
			if count == 0 {
				break
			}
			end := -1
			if count < 0 {
				if count == math.MinInt64 {
					return ErrBudget
				}
				count = -count
				size, e := c.Long()
				if e != nil || size < 0 || size > int64(len(c.Data)-c.Pos) {
					return ErrInvalid
				}
				end = c.Pos + int(size)
			}
			limit := int64(MaxItems)
			if n.Kind == "map" {
				limit = 32
			}
			if count > limit-total {
				return ErrBudget
			}
			total += count
			for i := int64(0); i < count; i++ {
				if n.Kind == "map" {
					key, e := c.String(64)
					if e != nil {
						return e
					}
					if keys[key] {
						return ErrInvalid
					}
					keys[key] = true
				}
				if e := c.Value(n.Items, depth+1); e != nil {
					return e
				}
			}
			if end >= 0 && c.Pos != end {
				return ErrInvalid
			}
		}
	default:
		return ErrInvalid
	}
	return nil
}
func Scan(plan *Node, b []byte) error {
	if len(b) > MaxWire {
		return ErrBudget
	}
	c := Cursor{Data: b}
	if e := c.Value(plan, 0); e != nil {
		return e
	}
	if c.Pos != len(b) {
		return ErrInvalid
	}
	return nil
}
