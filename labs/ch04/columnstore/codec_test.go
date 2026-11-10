package columnstore

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math/rand"
	"reflect"
	"testing"
)

func fixture(i int) Fact {
	c := []string{"tools", "food", "", "returns"}[i%4]
	u, n := int64(i%9-2), int64(i*17%10000)
	r := Fact{ID: uint64(i + 1), Version: 1, Tenant: []string{"acme", "globex"}[i%2], Warehouse: []string{"east", "west"}[(i/2)%2], Category: &c, Day: int64(i / 16), Units: &u, NetCents: &n}
	if i%7 == 0 {
		r.Category = nil
	}
	if i%5 == 0 {
		r.Units = nil
	}
	if i%11 == 0 {
		r.NetCents = nil
	}
	return r
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func TestSegmentEncodingsRoundTripAndDetectDamage(t *testing.T) {
	var rows []Fact
	for i := 0; i < 193; i++ {
		rows = append(rows, fixture(i))
	}
	for _, style := range []StringCodec{Auto, RLE, Bitmap} {
		t.Run(string(style), func(t *testing.T) {
			b, sorted, e := encodeSegment(rows, style)
			must(t, e)
			got, e := validateSegment(b)
			must(t, e)
			if !reflect.DeepEqual(got, sorted) {
				t.Fatal("column round trip")
			}
			for _, off := range []int{0, 8, 12, 24, 31, 32, len(b) / 2, len(b) - 1} {
				bad := append([]byte(nil), b...)
				bad[off] ^= 1
				if _, e = validateSegment(bad); e == nil {
					t.Fatal("damage accepted", off)
				}
			}
			for _, size := range []int{0, 1, 31, 32, len(b) - 1} {
				if _, e = validateSegment(b[:size]); e == nil {
					t.Fatal("truncation accepted", size)
				}
			}
		})
	}
}
func TestColumnBoundsAfterValidChecksum(t *testing.T) {
	values := make([]Value, 64)
	for i := range values {
		if i%3 == 0 {
			values[i] = null("string")
		} else {
			values[i] = text("a")
		}
	}
	for _, style := range []StringCodec{RLE, Bitmap} {
		b, e := columnFrame(Category, values, style)
		must(t, e)
		for offset := 0; offset < len(b)-4; offset++ {
			bad := append([]byte(nil), b...)
			bad[offset] ^= 255
			binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], checksum))
			v, e := decodeColumn(Category, bad, 64)
			if e == nil {
				if len(v.Values) != 64 {
					t.Fatal("unbounded decoded length")
				}
				if _, e = columnFrame(Category, v.Values, style); e != nil {
					t.Fatal("decoded values cannot encode", e)
				}
			}
		}
	}
}
func FuzzColumnDecoder(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4})
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		v := make([]Value, 64)
		for i := range v {
			v[i] = text([]string{"a", "b", "c"}[i%3])
			if i%7 == 0 {
				v[i] = null("string")
			}
		}
		b, _ := columnFrame(Category, v, Bitmap)
		for i := 0; i+1 < len(input) && i < 512; i += 2 {
			off := int(input[i]) % len(b)
			b[off] ^= input[i+1]
		}
		binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], checksum))
		if got, e := decodeColumn(Category, b, 64); e == nil {
			if len(got.Values) != 64 {
				t.Fatal("row bounds")
			}
			if _, e = columnFrame(Category, got.Values, Bitmap); e != nil {
				t.Fatal(e)
			}
		}
		_, _ = decodeColumn(Units, input, 64)
	})
}
func TestBatchTruthMatchesScalar(t *testing.T) {
	expressions := []string{"units >= 0 AND (category = 'tools' OR category IS NULL)", "NOT (units > 1 OR net_cents = NULL)", "units IS NULL OR NOT (category != 'food')", "(units = 0 OR units IS NULL) AND net_cents IS NOT NULL", "true", "false", "category = ''"}
	values := map[Column]vector{}
	rng := rand.New(rand.NewSource(404))
	for _, c := range columns {
		var vs []Value
		for i := 0; i < 64; i++ {
			vs = append(vs, fixture(rng.Intn(200)).value(c))
		}
		b, e := columnFrame(c, vs, Auto)
		must(t, e)
		values[c], e = decodeColumn(c, b, 64)
		must(t, e)
	}
	for _, source := range expressions {
		e, err := Parse(source)
		must(t, err)
		m := evalBatch(e.root, values, mask(64))
		for i := 0; i < 64; i++ {
			want := eval(e.root, func(c Column) Value { return values[c].Values[i] })
			got := falseValue
			if m.T&(uint64(1)<<i) != 0 {
				got = trueValue
			} else if m.U&(uint64(1)<<i) != 0 {
				got = unknownValue
			}
			if got != want {
				t.Fatal(source, i, got, want)
			}
		}
	}
}

func TestChecksumValidPruningMetadataCannotHideRows(t *testing.T) {
	var rows []Fact
	for i := 0; i < 64; i++ {
		rows = append(rows, fixture(i))
	}
	b, _, e := encodeSegment(rows, Auto)
	must(t, e)
	s, e := readCatalog(b)
	must(t, e)
	d := s.Groups[0].Columns[Units]
	d.Zone.Nulls++
	s.Groups[0].Columns[Units] = d
	meta, e := json.Marshal(s.catalog)
	must(t, e)
	bad := append([]byte(nil), b[:32]...)
	bad = append(bad, meta...)
	bad = append(bad, b[s.payload:]...)
	binary.LittleEndian.PutUint32(bad[8:], uint32(len(bad)))
	binary.LittleEndian.PutUint32(bad[12:], uint32(len(meta)))
	binary.LittleEndian.PutUint32(bad[24:], crc32.Checksum(meta, checksum))
	binary.LittleEndian.PutUint32(bad[28:], crc32.Checksum(bad[:28], checksum))
	if _, e = readCatalog(bad); e != nil {
		t.Fatal("fixture should have valid framing", e)
	}
	if _, e = validateSegment(bad); e == nil {
		t.Fatal("invalid pruning statistics accepted")
	}
}

func FuzzSegmentDecoder(f *testing.F) {
	var rows []Fact
	for i := 0; i < 64; i++ {
		rows = append(rows, fixture(i))
	}
	seed, _, _ := encodeSegment(rows, Auto)
	s, _ := readCatalog(seed)
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{12, 34, 255, 55, 72, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		b := append([]byte(nil), seed...)
		for i := 0; i+2 < len(input) && i < 768; i += 3 {
			offset := (int(input[i])<<8 | int(input[i+1])) % len(b)
			if offset >= 32 {
				b[offset] ^= input[i+2]
			}
		}
		binary.LittleEndian.PutUint32(b[24:], crc32.Checksum(b[32:s.payload], checksum))
		binary.LittleEndian.PutUint32(b[28:], crc32.Checksum(b[:28], checksum))
		for _, g := range s.Groups {
			for _, c := range columns {
				block := g.Columns[c]
				start, end := s.payload+int(block.Offset), s.payload+int(block.Offset+block.Length)
				binary.LittleEndian.PutUint32(b[end-4:], crc32.Checksum(b[start:end-4], checksum))
			}
		}
		if got, e := validateSegment(b); e == nil {
			if len(got) != 64 {
				t.Fatal("segment row bound")
			}
			for _, r := range got {
				if !r.valid() {
					t.Fatal("invalid decoded fact")
				}
			}
		}
		_, _ = validateSegment(input)
	})
}
