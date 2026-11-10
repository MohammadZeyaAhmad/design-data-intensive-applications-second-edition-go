package columnstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"reflect"
	"sort"
	"unicode/utf8"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

type StringCodec string

const (
	Auto   StringCodec = "auto"
	RLE    StringCodec = "rle"
	Bitmap StringCodec = "bitmap"
)
const (
	plain64    byte = 1
	intRLE     byte = 2
	dictRLE    byte = 3
	dictBitmap byte = 4
)

var checksum = crc32.MakeTable(crc32.Castagnoli)
var segmentMagic = []byte("ATCOL001")

type zone struct {
	Nulls    int `json:"nulls"`
	Min, Max Value
}
type block struct {
	Offset, Length uint32
	Codec          byte
	Zone           zone
}
type group struct {
	Rows    int
	Columns map[Column]block
}
type catalog struct {
	Rows   int
	Sorted bool
	Groups []group
}
type segment struct {
	catalog
	data    []byte
	payload int
}

func zoneOf(values []Value) zone {
	z := zone{Min: null(values[0].Kind), Max: null(values[0].Kind)}
	for _, v := range values {
		if !v.Valid {
			z.Nulls++
			continue
		}
		if !z.Min.Valid || compare(v, z.Min) < 0 {
			z.Min = v
		}
		if !z.Max.Valid || compare(v, z.Max) > 0 {
			z.Max = v
		}
	}
	return z
}
func columnFrame(c Column, values []Value, style StringCodec) ([]byte, error) {
	n := len(values)
	if n < 1 || n > GroupRows || kind(c) == "" {
		return nil, kv.ErrInvalid
	}
	var valid uint64
	for i, v := range values {
		if v.Kind != kind(c) || !v.Valid && !nullable(c) {
			return nil, kv.ErrInvalid
		}
		if v.Valid {
			valid |= uint64(1) << i
		}
	}
	codec := plain64
	var payload []byte
	if kind(c) == "int" {
		var plain, runs []byte
		for _, v := range values {
			x := int64(0)
			if v.Valid {
				x = v.I
			}
			plain = binary.LittleEndian.AppendUint64(plain, uint64(x))
		}
		for i := 0; i < n; {
			j := i + 1
			for j < n && bytes.Equal(plain[i*8:i*8+8], plain[j*8:j*8+8]) {
				j++
			}
			runs = append(runs, byte(j-i))
			runs = append(runs, plain[i*8:i*8+8]...)
			i = j
		}
		payload = plain
		if len(runs) < len(plain) {
			payload = runs
			codec = intRLE
		}
	} else {
		if style != Auto && style != RLE && style != Bitmap {
			return nil, kv.ErrInvalid
		}
		set := map[string]bool{}
		for _, v := range values {
			if v.Valid {
				if len(v.S) > 32 || !utf8.ValidString(v.S) {
					return nil, kv.ErrInvalid
				}
				set[v.S] = true
			}
		}
		var dictionary []string
		for s := range set {
			dictionary = append(dictionary, s)
		}
		sort.Strings(dictionary)
		dict := binary.LittleEndian.AppendUint16(nil, uint16(len(dictionary)))
		ids := map[string]uint16{}
		for i, s := range dictionary {
			dict = binary.LittleEndian.AppendUint16(dict, uint16(len(s)))
			dict = append(dict, s...)
			ids[s] = uint16(i + 1)
		}
		codes := make([]uint16, n)
		members := make([]uint64, len(dictionary))
		for i, v := range values {
			if v.Valid {
				codes[i] = ids[v.S]
				members[codes[i]-1] |= uint64(1) << i
			}
		}
		runs := append([]byte(nil), dict...)
		for i := 0; i < n; {
			j := i + 1
			for j < n && codes[j] == codes[i] {
				j++
			}
			runs = append(runs, byte(j-i))
			runs = binary.LittleEndian.AppendUint16(runs, codes[i])
			i = j
		}
		bits := append([]byte(nil), dict...)
		for _, m := range members {
			bits = binary.LittleEndian.AppendUint64(bits, m)
		}
		payload = runs
		codec = dictRLE
		if style == Bitmap || style == Auto && len(bits) < len(runs) {
			payload = bits
			codec = dictBitmap
		}
	}
	b := make([]byte, 16)
	b[0] = codec
	if kind(c) == "string" {
		b[1] = 1
	}
	binary.LittleEndian.PutUint16(b[2:], uint16(n))
	binary.LittleEndian.PutUint64(b[4:], valid)
	binary.LittleEndian.PutUint32(b[12:], uint32(len(payload)))
	b = append(b, payload...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, checksum))
	return b, nil
}
func decodeColumn(c Column, b []byte, n int) (vector, error) {
	var v vector
	if kind(c) == "" || n < 1 || n > GroupRows || len(b) < 20 || len(b) > 8192 {
		return v, kv.ErrCorrupt
	}
	codec := b[0]
	stringFlag := byte(0)
	if kind(c) == "string" {
		stringFlag = 1
	}
	if b[1] != stringFlag || int(binary.LittleEndian.Uint16(b[2:])) != n || int(binary.LittleEndian.Uint32(b[12:])) != len(b)-20 || crc32.Checksum(b[:len(b)-4], checksum) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return v, kv.ErrCorrupt
	}
	v.Valid = binary.LittleEndian.Uint64(b[4:])
	if v.Valid&^mask(n) != 0 || !nullable(c) && v.Valid != mask(n) {
		return v, kv.ErrCorrupt
	}
	p := b[16 : len(b)-4]
	v.Values = make([]Value, n)
	for i := range v.Values {
		v.Values[i] = null(kind(c))
	}
	v.Equal = map[string]uint64{}
	if kind(c) == "int" {
		var numbers []int64
		switch codec {
		case plain64:
			if len(p) != n*8 {
				return vector{}, kv.ErrCorrupt
			}
			for len(p) > 0 {
				numbers = append(numbers, int64(binary.LittleEndian.Uint64(p)))
				p = p[8:]
			}
		case intRLE:
			for len(p) > 0 {
				if len(p) < 9 {
					return vector{}, kv.ErrCorrupt
				}
				count := int(p[0])
				x := int64(binary.LittleEndian.Uint64(p[1:]))
				p = p[9:]
				if count == 0 || len(numbers)+count > n || len(numbers) > 0 && numbers[len(numbers)-1] == x {
					return vector{}, kv.ErrCorrupt
				}
				for j := 0; j < count; j++ {
					numbers = append(numbers, x)
				}
			}
		default:
			return vector{}, kv.ErrCorrupt
		}
		if len(numbers) != n {
			return vector{}, kv.ErrCorrupt
		}
		for i, x := range numbers {
			if v.Valid&(uint64(1)<<i) != 0 {
				v.Values[i] = number(x)
			} else if x != 0 {
				return vector{}, kv.ErrCorrupt
			}
		}
	} else {
		if codec != dictRLE && codec != dictBitmap || len(p) < 2 {
			return vector{}, kv.ErrCorrupt
		}
		count := int(binary.LittleEndian.Uint16(p))
		p = p[2:]
		if count > n {
			return vector{}, kv.ErrCorrupt
		}
		dictionary := make([]string, 0, count)
		for j := 0; j < count; j++ {
			if len(p) < 2 {
				return vector{}, kv.ErrCorrupt
			}
			size := int(binary.LittleEndian.Uint16(p))
			p = p[2:]
			if size > 32 || size > len(p) {
				return vector{}, kv.ErrCorrupt
			}
			s := string(p[:size])
			p = p[size:]
			if !utf8.ValidString(s) || j > 0 && dictionary[j-1] >= s {
				return vector{}, kv.ErrCorrupt
			}
			dictionary = append(dictionary, s)
		}
		if codec == dictBitmap {
			if len(p) != count*8 {
				return vector{}, kv.ErrCorrupt
			}
			var seen uint64
			for _, s := range dictionary {
				m := binary.LittleEndian.Uint64(p)
				p = p[8:]
				if m == 0 || m&^v.Valid != 0 || seen&m != 0 {
					return vector{}, kv.ErrCorrupt
				}
				seen |= m
				v.Equal[s] = m
				for i := 0; i < n; i++ {
					if m&(uint64(1)<<i) != 0 {
						v.Values[i] = text(s)
					}
				}
			}
			if seen != v.Valid {
				return vector{}, kv.ErrCorrupt
			}
		} else {
			pos := 0
			previous := -1
			for len(p) > 0 {
				if len(p) < 3 {
					return vector{}, kv.ErrCorrupt
				}
				run, code := int(p[0]), int(binary.LittleEndian.Uint16(p[1:]))
				p = p[3:]
				if run == 0 || pos+run > n || code > count || code == previous {
					return vector{}, kv.ErrCorrupt
				}
				previous = code
				for j := 0; j < run; j++ {
					valid := v.Valid&(uint64(1)<<pos) != 0
					if (code != 0) != valid {
						return vector{}, kv.ErrCorrupt
					}
					if code > 0 {
						s := dictionary[code-1]
						v.Values[pos] = text(s)
						v.Equal[s] |= uint64(1) << pos
					}
					pos++
				}
			}
			if pos != n || len(v.Equal) != count {
				return vector{}, kv.ErrCorrupt
			}
		}
	}
	return v, nil
}
func factLess(a, b Fact) bool {
	for _, c := range []Column{Tenant, Warehouse, Day, Category, ID} {
		x, y := a.value(c), b.value(c)
		if x.Valid != y.Valid {
			return !x.Valid
		}
		if n := compare(x, y); n != 0 {
			return n < 0
		}
	}
	return false
}
func encodeSegment(rows []Fact, style StringCodec) ([]byte, []Fact, error) {
	return buildSegment(rows, style, true)
}
func buildSegment(rows []Fact, style StringCodec, ordered bool) ([]byte, []Fact, error) {
	if len(rows) == 0 || len(rows) > SegmentRows {
		return nil, nil, kv.ErrInvalid
	}
	r := append([]Fact(nil), rows...)
	if ordered {
		sort.Slice(r, func(i, j int) bool { return factLess(r[i], r[j]) })
	}
	seen := map[uint64]bool{}
	for _, row := range r {
		if !row.valid() || seen[row.ID] {
			return nil, nil, kv.ErrInvalid
		}
		seen[row.ID] = true
	}
	c := catalog{Rows: len(r), Sorted: ordered}
	var payload []byte
	for start := 0; start < len(r); start += GroupRows {
		end := min(start+GroupRows, len(r))
		g := group{Rows: end - start, Columns: map[Column]block{}}
		for _, col := range columns {
			values := make([]Value, 0, g.Rows)
			for _, row := range r[start:end] {
				values = append(values, row.value(col))
			}
			b, e := columnFrame(col, values, style)
			if e != nil {
				return nil, nil, e
			}
			g.Columns[col] = block{Offset: uint32(len(payload)), Length: uint32(len(b)), Codec: b[0], Zone: zoneOf(values)}
			payload = append(payload, b...)
		}
		c.Groups = append(c.Groups, g)
	}
	meta, e := json.Marshal(c)
	if e != nil {
		return nil, nil, e
	}
	b := make([]byte, 32)
	copy(b, segmentMagic)
	binary.LittleEndian.PutUint32(b[8:], uint32(32+len(meta)+len(payload)))
	binary.LittleEndian.PutUint32(b[12:], uint32(len(meta)))
	binary.LittleEndian.PutUint32(b[16:], uint32(len(r)))
	binary.LittleEndian.PutUint32(b[20:], uint32(len(c.Groups)))
	binary.LittleEndian.PutUint32(b[24:], crc32.Checksum(meta, checksum))
	binary.LittleEndian.PutUint32(b[28:], crc32.Checksum(b[:28], checksum))
	b = append(b, meta...)
	b = append(b, payload...)
	if len(b) > maxSegmentBytes {
		return nil, nil, kv.ErrLimit
	}
	return b, r, nil
}
func readCatalog(b []byte) (segment, error) {
	var s segment
	if len(b) < 32 || len(b) > maxSegmentBytes || !bytes.Equal(b[:8], segmentMagic) || int(binary.LittleEndian.Uint32(b[8:])) != len(b) || crc32.Checksum(b[:28], checksum) != binary.LittleEndian.Uint32(b[28:]) {
		return s, kv.ErrCorrupt
	}
	metaLen := int(binary.LittleEndian.Uint32(b[12:]))
	if metaLen < 1 || metaLen > 1<<20 || metaLen > len(b)-32 {
		return s, kv.ErrCorrupt
	}
	meta := b[32 : 32+metaLen]
	if crc32.Checksum(meta, checksum) != binary.LittleEndian.Uint32(b[24:]) || json.Unmarshal(meta, &s.catalog) != nil || s.Rows < 1 || s.Rows > SegmentRows || s.Rows != int(binary.LittleEndian.Uint32(b[16:])) || len(s.Groups) != (s.Rows+GroupRows-1)/GroupRows || len(s.Groups) != int(binary.LittleEndian.Uint32(b[20:])) {
		return segment{}, kv.ErrCorrupt
	}
	pos, total := uint64(0), 0
	for i, g := range s.Groups {
		if g.Rows < 1 || g.Rows > GroupRows || i < len(s.Groups)-1 && g.Rows != GroupRows || len(g.Columns) != len(columns) {
			return segment{}, kv.ErrCorrupt
		}
		total += g.Rows
		for _, col := range columns {
			d, ok := g.Columns[col]
			if !ok || uint64(d.Offset) != pos || d.Length < 20 || d.Length > 8192 {
				return segment{}, kv.ErrCorrupt
			}
			pos += uint64(d.Length)
			if pos > uint64(len(b)-32-metaLen) {
				return segment{}, kv.ErrCorrupt
			}
		}
	}
	if total != s.Rows || pos != uint64(len(b)-32-metaLen) {
		return segment{}, kv.ErrCorrupt
	}
	s.data = b
	s.payload = 32 + metaLen
	return s, nil
}
func (s segment) column(g int, c Column) (vector, error) {
	d := s.Groups[g].Columns[c]
	b := s.data[s.payload+int(d.Offset) : s.payload+int(d.Offset+d.Length)]
	if b[0] != d.Codec {
		return vector{}, kv.ErrCorrupt
	}
	return decodeColumn(c, b, s.Groups[g].Rows)
}
func validateSegment(b []byte) ([]Fact, error) {
	s, e := readCatalog(b)
	if e != nil {
		return nil, e
	}
	var rows []Fact
	seen := map[uint64]bool{}
	for g, group := range s.Groups {
		values := map[Column][]Value{}
		for _, c := range columns {
			v, e := s.column(g, c)
			if e != nil {
				return nil, e
			}
			if !reflect.DeepEqual(zoneOf(v.Values), group.Columns[c].Zone) {
				return nil, kv.ErrCorrupt
			}
			values[c] = v.Values
		}
		for i := 0; i < group.Rows; i++ {
			r, e := factFrom(values, i)
			if e != nil || seen[r.ID] || s.Sorted && len(rows) > 0 && !factLess(rows[len(rows)-1], r) {
				return nil, kv.ErrCorrupt
			}
			seen[r.ID] = true
			rows = append(rows, r)
		}
	}
	return rows, nil
}
func possible(n *node, g group) bool {
	switch n.Op {
	case "true":
		return true
	case "false":
		return false
	case "and":
		return possible(n.Left, g) && possible(n.Right, g)
	case "or":
		return possible(n.Left, g) || possible(n.Right, g)
	case "not":
		return true
	}
	z := g.Columns[n.Column].Zone
	if n.Op == "isnull" {
		return z.Nulls > 0
	}
	if n.Op == "notnull" {
		return z.Nulls < g.Rows
	}
	if !n.Literal.Valid || !z.Min.Valid {
		return false
	}
	lo, hi := compare(z.Min, n.Literal), compare(z.Max, n.Literal)
	switch n.Op {
	case "=":
		return lo <= 0 && hi >= 0
	case "!=":
		return lo != 0 || hi != 0
	case "<":
		return lo < 0
	case "<=":
		return lo <= 0
	case ">":
		return hi > 0
	case ">=":
		return hi >= 0
	}
	return true
}
