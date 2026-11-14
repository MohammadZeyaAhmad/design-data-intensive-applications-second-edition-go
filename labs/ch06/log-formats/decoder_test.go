package logformats

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

func put(b *bytes.Buffer, value any) { _ = binary.Write(b, binary.BigEndian, value) }
func beginFrame(lsn uint64) []byte {
	b := &bytes.Buffer{}
	b.WriteByte('B')
	put(b, lsn)
	put(b, uint64(0))
	put(b, uint32(7))
	return b.Bytes()
}
func commitFrame(lsn uint64) []byte {
	b := &bytes.Buffer{}
	b.WriteByte('C')
	b.WriteByte(0)
	put(b, lsn)
	put(b, lsn+8)
	put(b, uint64(0))
	return b.Bytes()
}
func relationFrame() []byte {
	b := &bytes.Buffer{}
	b.WriteByte('R')
	put(b, uint32(42))
	b.WriteString("log_lab\x00scratch\x00")
	b.WriteByte('d')
	put(b, uint16(2))
	for _, c := range []struct {
		name string
		oid  uint32
		key  byte
	}{{"id", 25, 1}, {"quantity", 20, 0}} {
		b.WriteByte(c.key)
		b.WriteString(c.name)
		b.WriteByte(0)
		put(b, c.oid)
		put(b, int32(-1))
	}
	return b.Bytes()
}
func tupleFrame(cells ...Cell) []byte {
	b := &bytes.Buffer{}
	put(b, uint16(len(cells)))
	for _, cell := range cells {
		b.WriteByte(cell.Kind)
		if cell.Kind == 't' {
			put(b, uint32(len(cell.Text)))
			b.WriteString(cell.Text)
		}
	}
	return b.Bytes()
}
func insertFrame(id, quantity string) []byte {
	b := &bytes.Buffer{}
	b.WriteByte('I')
	put(b, uint32(42))
	b.WriteByte('N')
	b.Write(tupleFrame(Cell{'t', id}, Cell{'t', quantity}))
	return b.Bytes()
}
func updateFrame(id, quantity string) []byte {
	b := &bytes.Buffer{}
	b.WriteByte('U')
	put(b, uint32(42))
	b.WriteByte('N')
	b.Write(tupleFrame(Cell{'t', id}, Cell{'t', quantity}))
	return b.Bytes()
}
func deleteFrame(id string) []byte {
	b := &bytes.Buffer{}
	b.WriteByte('D')
	put(b, uint32(42))
	b.WriteByte('K')
	b.Write(tupleFrame(Cell{'t', id}, Cell{Kind: 'n'}))
	return b.Bytes()
}
func TestDecoderPublishesOnlyCompleteOrderedTransactions(t *testing.T) {
	d := NewDecoder()
	frames := [][]byte{beginFrame(100), relationFrame(), insertFrame("a", "1"), updateFrame("a", "2"), deleteFrame("a")}
	for _, frame := range frames {
		result, err := d.Feed(frame)
		if err != nil || result != nil {
			t.Fatal("partial transaction escaped", result, err)
		}
	}
	batch, err := d.Feed(commitFrame(100))
	if err != nil || batch.End != 108 || batch.XID != 7 {
		t.Fatal(batch, err)
	}
	kinds := []string{}
	for _, change := range batch.Changes {
		kinds = append(kinds, change.Kind)
	}
	if !reflect.DeepEqual(kinds, []string{"insert", "update", "delete"}) {
		t.Fatal(kinds)
	}
	if _, err = d.Feed(commitFrame(100)); !errors.Is(err, ErrProtocol) {
		t.Fatal("commit without begin accepted")
	}
}
func TestDecoderSchedulesMatchIndependentStateMap(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		rng := rand.New(rand.NewSource(seed))
		d := NewDecoder()
		expected, actual := map[string]string{}, map[string]string{}
		if _, err := d.Feed(relationFrame()); err != nil {
			t.Fatal(err)
		}
		for tx := range 24 {
			lsn := uint64(100 + tx*100)
			_, _ = d.Feed(beginFrame(lsn))
			for range 8 {
				key := fmt.Sprint("key", rng.Intn(8))
				value := fmt.Sprint(rng.Intn(100))
				var frame []byte
				if _, exists := expected[key]; !exists {
					frame = insertFrame(key, value)
					expected[key] = value
				} else if rng.Intn(3) == 0 {
					frame = deleteFrame(key)
					delete(expected, key)
				} else {
					frame = updateFrame(key, value)
					expected[key] = value
				}
				if batch, err := d.Feed(frame); err != nil || batch != nil {
					t.Fatal("transaction leaked or decode failed", err)
				}
			}
			batch, err := d.Feed(commitFrame(lsn))
			if err != nil {
				t.Fatal(err)
			}
			for _, change := range batch.Changes {
				switch change.Kind {
				case "insert", "update":
					actual[change.New[0].Text] = change.New[1].Text
				case "delete":
					delete(actual, change.Old[0].Text)
				}
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("seed %d tx %d differs from oracle", seed, tx)
			}
		}
	}
}
func TestMalformedPrefixesUnknownRelationsAndBounds(t *testing.T) {
	for _, frame := range [][]byte{beginFrame(100), commitFrame(100), relationFrame(), insertFrame("a", "1"), updateFrame("a", "2"), deleteFrame("a")} {
		for n := 0; n < len(frame); n++ {
			_, _ = NewDecoder().Feed(frame[:n])
		}
	}
	d := NewDecoder()
	_, _ = d.Feed(beginFrame(100))
	if _, err := d.Feed(insertFrame("a", "1")); !errors.Is(err, ErrProtocol) {
		t.Fatal("unannounced relation accepted")
	}
	d = NewDecoder()
	_, _ = d.Feed(relationFrame())
	_, _ = d.Feed(beginFrame(100))
	for range MaxChanges {
		if _, err := d.Feed(insertFrame("a", "1")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Feed(insertFrame("a", "1")); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded transaction changes")
	}
	if _, err := NewDecoder().Feed(make([]byte, MaxFrameBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatal("frame budget ignored")
	}
	d = NewDecoder()
	_, _ = d.Feed(beginFrame(100))
	if _, err := d.Feed(beginFrame(200)); !errors.Is(err, ErrProtocol) {
		t.Fatal("nested begin accepted")
	}
	d = NewDecoder()
	_, _ = d.Feed(beginFrame(100))
	if _, err := d.Feed(commitFrame(200)); !errors.Is(err, ErrProtocol) {
		t.Fatal("wrong commit identity accepted")
	}
}
func TestWireCountsAreCheckedBeforeAllocation(t *testing.T) {
	truncate := []byte{'T', 255, 255, 255, 255, 0, 0, 0, 0, 1}
	if _, err := parse(truncate); !errors.Is(err, ErrLimit) {
		t.Fatal("forged truncate count accepted", err)
	}
	insert := insertFrame("a", "1")
	binary.BigEndian.PutUint32(insert[9:13], 0xffffffff)
	if _, err := parse(insert); err == nil {
		t.Fatal("forged tuple length accepted")
	}
	if _, err := parse(append(beginFrame(100), 0)); !errors.Is(err, ErrProtocol) {
		t.Fatal("trailing wire bytes accepted")
	}
}
func TestBufferedTransactionByteBudgetIsIndependentOfChangeCount(t *testing.T) {
	d := NewDecoder()
	_, _ = d.Feed(relationFrame())
	_, _ = d.Feed(beginFrame(100))
	frame := insertFrame("a", strings.Repeat("x", MaxFrameBytes/2))
	for range 7 {
		if _, err := d.Feed(frame); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Feed(frame); !errors.Is(err, ErrLimit) {
		t.Fatal("buffered bytes exceeded transaction budget", err)
	}
}
func TestNullEmptyAndUnchangedToastStayDistinct(t *testing.T) {
	r := Relation{Columns: []Column{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	tupleData := &pglogrepl.TupleData{ColumnNum: 3, Columns: []*pglogrepl.TupleDataColumn{{DataType: 'n'}, {DataType: 't', Data: []byte{}}, {DataType: 'u'}}}
	cells, err := tuple(tupleData, r, false)
	if err != nil || cells[0].Kind != 'n' || cells[1].Kind != 't' || cells[1].Text != "" || cells[2].Kind != 'u' {
		t.Fatal(cells, err)
	}
	if _, err = tuple(tupleData, r, true); !errors.Is(err, ErrProtocol) {
		t.Fatal("incomplete insert accepted")
	}
	tupleData.Columns[2].DataType = 'b'
	if _, err = tuple(tupleData, r, false); !errors.Is(err, ErrProtocol) {
		t.Fatal("binary encoding accepted without contract")
	}
}
func TestLogicalSchemaRequiresExplicitTargetContract(t *testing.T) {
	r := Relation{Namespace: "log_lab", Name: "scratch", Columns: []Column{{Name: "id", OID: 25, Modifier: -1, Key: true}, {Name: "quantity", OID: 20, Modifier: -1}}}
	target := r
	if err := match(r, target); err != nil {
		t.Fatal(err)
	}
	target.Columns = append([]Column(nil), r.Columns...)
	target.Columns[1].OID = 23
	if !errors.Is(match(r, target), ErrSchema) {
		t.Fatal("type narrowing accepted")
	}
	target.Columns = r.Columns[:1]
	if !errors.Is(match(r, target), ErrSchema) {
		t.Fatal("missing column silently projected")
	}
	target = r
	target.Namespace = "another_context"
	if !errors.Is(match(r, target), ErrSchema) {
		t.Fatal("table allowlist bypassed")
	}
}
func TestPositionsPreserveUint64Precision(t *testing.T) {
	p := Position(0xfffffffffffffff0)
	b, err := json.Marshal(p)
	if err != nil || b[0] != '"' {
		t.Fatal("LSN became a JSON number")
	}
	var roundtrip Position
	if json.Unmarshal(b, &roundtrip) != nil || roundtrip != p {
		t.Fatal("LSN precision lost")
	}
	for _, invalid := range []string{"1/100000000", "1/2 trailing", "1/", "-1/10", "0/0\x00"} {
		if _, err := ParsePosition(invalid); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid LSN accepted", invalid)
		}
	}
}
func TestPhysicalFormatAndLogicalSchemaHaveDifferentContracts(t *testing.T) {
	source := PhysicalContract{"cluster-a", 18, 8192, 202506291}
	if err := CompatiblePhysical(source, source); err != nil {
		t.Fatal(err)
	}
	for _, target := range []PhysicalContract{{"cluster-a", 17, 8192, 202506291}, {"cluster-b", 18, 8192, 202506291}, {"cluster-a", 18, 4096, 202506291}, {"cluster-a", 18, 8192, 202406291}} {
		if !errors.Is(CompatiblePhysical(source, target), ErrPhysical) {
			t.Fatal("physical incompatibility accepted", target)
		}
	}
}
func FuzzPGOutputFrames(f *testing.F) {
	for _, frame := range [][]byte{beginFrame(100), relationFrame(), insertFrame("a", "1"), updateFrame("a", "2"), deleteFrame("a"), commitFrame(100), {}} {
		f.Add(frame)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > MaxFrameBytes {
			return
		}
		d := NewDecoder()
		_, _ = d.Feed(relationFrame())
		_, _ = d.Feed(beginFrame(100))
		_, _ = d.Feed(frame)
	})
}
