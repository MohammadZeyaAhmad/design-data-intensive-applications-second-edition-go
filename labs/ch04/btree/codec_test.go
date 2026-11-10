package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func fixtureRedo(t testing.TB) []byte {
	t.Helper()
	id := [16]byte{1}
	m := metadata{ID: id, Generation: 1, Root: 1, Next: 2, Keys: 1, Order: 8, Height: 1}
	meta, e := encodeMeta(m)
	if e != nil {
		t.Fatal(e)
	}
	leaf, e := encodePage(&page{id: 1, lsn: 1, kind: kindLeaf, keys: [][]byte{[]byte("a")}, values: [][]byte{[]byte("value")}}, 8)
	if e != nil {
		t.Fatal(e)
	}
	b, e := encodeRedo(redo{id: id, generation: 1, meta: m, images: map[uint64][]byte{0: meta, 1: leaf}})
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestEveryIncompleteRedoPrefixAndCompleteCorruption(t *testing.T) {
	frame := fixtureRedo(t)
	for cut := 1; cut < len(frame); cut++ {
		if _, e := decodeRedo(frame[:cut]); !errors.Is(e, errTail) {
			t.Fatalf("prefix %d: %v", cut, e)
		}
	}
	for _, offset := range []int{0, 8, 12, 16, 24, 28, 32, len(frame) - 1} {
		b := clone(frame)
		b[offset] ^= 1
		if _, e := decodeRedo(b); !errors.Is(e, ErrCorrupt) {
			t.Fatal("damage accepted", offset, e)
		}
	}
	b := clone(frame)
	binary.LittleEndian.PutUint32(b[12:], maxDirtyPages+1)
	binary.LittleEndian.PutUint32(b[28:], crc32.Checksum(b[:28], crcTable))
	if _, e := decodeRedo(b); !errors.Is(e, ErrCorrupt) {
		t.Fatal("oversized image count", e)
	}
}
func TestTruncatedUncommittedRedoLeavesStableTree(t *testing.T) {
	for _, cut := range []int{1, 31, 32, 33, 4095, 4096, 5000, 8200} {
		t.Run(string(rune('a'+cut%26)), func(t *testing.T) {
			dir := t.TempDir()
			fault := false
			d := mustOpen(t, dir, Options{Failpoint: func(p string) error {
				if fault && p == "wal.written" {
					return errors.New("append lost")
				}
				return nil
			}})
			must(t, d.Put([]byte("a"), []byte("base")))
			fault = true
			if e := d.Put([]byte("a"), []byte("unknown")); !errors.Is(e, ErrRecovery) {
				t.Fatal(e)
			}
			must(t, d.Close())
			path := filepath.Join(dir, "WAL")
			b, e := os.ReadFile(path)
			must(t, e)
			if cut >= len(b) {
				t.Fatal("bad fixture bound")
			}
			must(t, os.WriteFile(path, b[:cut], 0600))
			d = mustOpen(t, dir, Options{})
			defer d.Close()
			assertMap(t, d, map[string]string{"a": "base"})
			s, e := os.Stat(path)
			must(t, e)
			if s.Size() != 0 {
				t.Fatal("incomplete tail not cleared")
			}
		})
	}
}
func TestPageBoundsAndCanonicalEncoding(t *testing.T) {
	n := &page{id: 1, kind: kindLeaf, keys: [][]byte{[]byte("a")}, values: [][]byte{nil}}
	b, e := encodePage(n, 8)
	must(t, e)
	decoded, e := decodePage(b, 1, 8)
	must(t, e)
	again, e := encodePage(decoded, 8)
	must(t, e)
	if !bytes.Equal(b, again) {
		t.Fatal("page round trip")
	}
	// A checksum-valid out-of-range slot count must still fail before allocation.
	binary.LittleEndian.PutUint16(b[28:], 65535)
	binary.LittleEndian.PutUint32(b[pageCRC:], crc32.Checksum(b[:pageCRC], crcTable))
	if _, e = decodePage(b, 1, 8); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
}

// Structure-aware fuzzing fixes framing checksums so mutations reach slot,
// pointer and metadata validation instead of stopping at a checksum mismatch.
func pageRoundTrip(t testing.TB, b []byte) {
	t.Helper()
	n, e := decodePage(b, 1, 8)
	if e == nil {
		again, e := encodePage(n, 8)
		if e != nil || !bytes.Equal(again, b) {
			t.Fatal("noncanonical decoded page", e)
		}
	}
}
func redoRoundTrip(t testing.TB, b []byte) {
	t.Helper()
	r, e := decodeRedo(b)
	if e == nil {
		again, e := encodeRedo(r)
		if e != nil || !bytes.Equal(again, b) {
			t.Fatal("redo round trip", e)
		}
	} else if errors.Is(e, io.EOF) && len(b) != 0 {
		t.Fatal("nonempty log became empty")
	}
}
func FuzzPageDecoder(f *testing.F) {
	for _, n := range []*page{{id: 1, kind: kindLeaf}, {id: 1, lsn: 5, kind: kindLeaf, keys: [][]byte{[]byte("a")}, values: [][]byte{[]byte("v")}}, {id: 1, lsn: 3, kind: kindBranch, keys: [][]byte{[]byte("m")}, children: []uint64{2, 3}}, {id: 1, kind: kindFree, next: 2}} {
		b, e := encodePage(n, 8)
		if e != nil {
			f.Fatal(e)
		}
		size := int(binary.LittleEndian.Uint16(b[30:]))
		f.Add(clone(b[:pageHeader+size]))
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > PageSize {
			t.Skip()
		}
		pageRoundTrip(t, input)
		b := make([]byte, PageSize)
		copy(b, input)
		copy(b, pageMagic)
		binary.LittleEndian.PutUint64(b[8:], 1)
		binary.LittleEndian.PutUint32(b[pageCRC:], crc32.Checksum(b[:pageCRC], crcTable))
		pageRoundTrip(t, b)
	})
}
func resealRedo(b []byte) {
	for index := 0; index < 2; index++ {
		start := logHeader + 16 + index*(PageSize+8) + 8
		binary.LittleEndian.PutUint32(b[start+pageCRC:], crc32.Checksum(b[start:start+pageCRC], crcTable))
	}
	binary.LittleEndian.PutUint32(b[24:], crc32.Checksum(b[logHeader:], crcTable))
	binary.LittleEndian.PutUint32(b[28:], crc32.Checksum(b[:28], crcTable))
}
func TestChecksumValidRedoMutationSweep(t *testing.T) {
	original := fixtureRedo(t)
	for offset := range original {
		b := clone(original)
		b[offset] ^= 1
		resealRedo(b)
		redoRoundTrip(t, b)
	}
}
func FuzzRedoDecoder(f *testing.F) {
	original := fixtureRedo(f)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0xff})
	f.Add([]byte{60, 16, 0xff})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1024 {
			t.Skip()
		}
		redoRoundTrip(t, input)
		b := clone(original)
		if len(input) >= 2 {
			offset := int(binary.LittleEndian.Uint16(input)) % len(b)
			copy(b[offset:], input[2:])
		}
		resealRedo(b)
		redoRoundTrip(t, b)
	})
}
