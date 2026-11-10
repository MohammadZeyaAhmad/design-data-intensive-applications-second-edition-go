package lsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

var tableMagic = []byte("ATLSST01")

type block struct {
	First  []byte `json:"first"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}
type tableIndex struct {
	Blocks         []block `json:"blocks"`
	Bloom          bloom   `json:"bloom"`
	Min, Max       []byte
	MinSeq, MaxSeq uint64
	Count          int
}
type descriptor struct {
	ID     uint64 `json:"id"`
	Level  int    `json:"level"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Min    []byte `json:"min"`
	Max    []byte `json:"max"`
	MinSeq uint64 `json:"min_seq"`
	MaxSeq uint64 `json:"max_seq"`
	Count  int    `json:"count"`
}
type table struct {
	desc  descriptor
	file  dataFile
	index tableIndex
	end   int64
}

func tableName(id uint64) string { return fmt.Sprintf("%020d.sst", id) }

func (d *DB) writeTable(rows []record, level int, op string) (descriptor, error) {
	var desc descriptor
	if len(rows) == 0 || len(rows) > maxEntries {
		return desc, ErrLimit
	}
	id := d.nextID
	if id == 0 || id == ^uint64(0) {
		return desc, ErrLimit
	}
	d.nextID++
	tmp := d.path(".tmp-" + tableName(id))
	final := d.path(tableName(id))
	f, e := d.fs.Open(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if e != nil {
		return desc, e
	}
	defer f.Close()
	h := sha256.New()
	write := func(b []byte) error {
		n, e := writeAll(f, b)
		h.Write(b[:n])
		d.count.TableBytes += uint64(n)
		return e
	}
	if e = write(tableMagic); e != nil {
		return desc, e
	}
	idx := tableIndex{Bloom: newBloom(len(rows)), Count: len(rows), Min: clone(rows[0].key), Max: clone(rows[len(rows)-1].key), MinSeq: ^uint64(0)}
	offset := int64(len(tableMagic))
	blockStart := offset
	var first []byte
	for i, r := range rows {
		if i > 0 && bytes.Compare(rows[i-1].key, r.key) >= 0 {
			return desc, corruption("unsorted table input")
		}
		if first == nil {
			first = clone(r.key)
		}
		b := encodeRecord(r)
		if e = write(b); e != nil {
			return desc, e
		}
		offset += int64(len(b))
		if offset-int64(len(tableMagic)) > maxDataBytes {
			return desc, ErrLimit
		}
		idx.Bloom.add(r.key)
		if r.seq < idx.MinSeq {
			idx.MinSeq = r.seq
		}
		if r.seq > idx.MaxSeq {
			idx.MaxSeq = r.seq
		}
		if offset-blockStart >= int64(d.opts.BlockBytes) {
			idx.Blocks = append(idx.Blocks, block{first, blockStart, offset - blockStart})
			blockStart = offset
			first = nil
		}
	}
	if first != nil {
		idx.Blocks = append(idx.Blocks, block{first, blockStart, offset - blockStart})
	}
	encoded, e := json.Marshal(idx)
	if e != nil {
		return desc, e
	}
	if len(encoded) > maxIndexBytes {
		return desc, ErrLimit
	}
	if e = write(encoded); e != nil {
		return desc, e
	}
	footer := make([]byte, 24)
	binary.LittleEndian.PutUint64(footer, uint64(offset))
	binary.LittleEndian.PutUint32(footer[8:], uint32(len(encoded)))
	binary.LittleEndian.PutUint32(footer[12:], crc32.Checksum(encoded, crcTable))
	copy(footer[16:], tableMagic)
	if e = write(footer); e != nil {
		return desc, e
	}
	if e = d.hit(op + ".table.written"); e != nil {
		return desc, e
	}
	if e = f.Sync(); e != nil {
		return desc, e
	}
	d.count.Syncs++
	if e = d.hit(op + ".table.synced"); e != nil {
		return desc, e
	}
	if e = f.Close(); e != nil {
		return desc, e
	}
	if e = d.fs.Rename(tmp, final); e != nil {
		return desc, e
	}
	if e = d.hit(op + ".table.renamed"); e != nil {
		return desc, e
	}
	desc = descriptor{ID: id, Level: level, Size: offset + int64(len(encoded)) + 24, SHA256: hex.EncodeToString(h.Sum(nil)), Min: idx.Min, Max: idx.Max, MinSeq: idx.MinSeq, MaxSeq: idx.MaxSeq, Count: idx.Count}
	return desc, nil
}

func (d *DB) openTable(desc descriptor) (t *table, err error) {
	f, e := d.fs.Open(d.path(tableName(desc.ID)), os.O_RDONLY)
	if e != nil {
		return nil, corruption("missing table %d", desc.ID)
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	size, e := f.Size()
	if e != nil {
		return nil, e
	}
	if size != desc.Size || size < 32 || size > maxFileBytes {
		return nil, corruption("table size %d", desc.ID)
	}
	h := sha256.New()
	if _, e = io.Copy(h, io.NewSectionReader(f, 0, size)); e != nil {
		return nil, e
	}
	d.count.VerifiedBytes += uint64(size)
	if hex.EncodeToString(h.Sum(nil)) != desc.SHA256 {
		return nil, corruption("table checksum %d", desc.ID)
	}
	start := make([]byte, 8)
	footer := make([]byte, 24)
	if _, e = f.ReadAt(start, 0); e != nil {
		return nil, e
	}
	if _, e = f.ReadAt(footer, size-24); e != nil {
		return nil, e
	}
	if !bytes.Equal(start, tableMagic) || !bytes.Equal(footer[16:], tableMagic) {
		return nil, corruption("table version %d", desc.ID)
	}
	offset := binary.LittleEndian.Uint64(footer)
	length := uint64(binary.LittleEndian.Uint32(footer[8:]))
	if offset < 8 || offset > maxDataBytes+8 || length == 0 || length > maxIndexBytes || offset+length != uint64(size-24) {
		return nil, corruption("table index bounds %d", desc.ID)
	}
	b := make([]byte, length)
	if _, e = f.ReadAt(b, int64(offset)); e != nil {
		return nil, e
	}
	if crc32.Checksum(b, crcTable) != binary.LittleEndian.Uint32(footer[12:]) {
		return nil, corruption("table index checksum %d", desc.ID)
	}
	var idx tableIndex
	if e = strictJSON(b, &idx); e != nil {
		return nil, corruption("table index encoding %d", desc.ID)
	}
	if idx.Count != desc.Count || idx.Count < 1 || idx.Count > maxEntries || idx.MinSeq != desc.MinSeq || idx.MaxSeq != desc.MaxSeq || !bytes.Equal(idx.Min, desc.Min) || !bytes.Equal(idx.Max, desc.Max) || len(idx.Blocks) == 0 || len(idx.Blocks) > idx.Count || idx.Bloom.Probes != 7 || len(idx.Bloom.Bits) != len(newBloom(idx.Count).Bits) {
		return nil, corruption("table index metadata %d", desc.ID)
	}
	end := int64(8)
	for i, blk := range idx.Blocks {
		if !validKey(blk.First) || blk.Offset != end || blk.Length < frameHeader+18 || blk.Length > maxRecordBytes+frameHeader+64<<10 || i > 0 && bytes.Compare(idx.Blocks[i-1].First, blk.First) >= 0 {
			return nil, corruption("sparse index %d", desc.ID)
		}
		end += blk.Length
		if end > int64(offset) {
			return nil, corruption("sparse index overflow %d", desc.ID)
		}
	}
	if end != int64(offset) || !bytes.Equal(idx.Blocks[0].First, idx.Min) {
		return nil, corruption("sparse index coverage %d", desc.ID)
	}
	return &table{desc: desc, file: f, index: idx, end: end}, nil
}

type countedReader struct {
	io.ReaderAt
	count *uint64
}

func (r countedReader) ReadAt(b []byte, o int64) (int, error) {
	n, e := r.ReaderAt.ReadAt(b, o)
	*r.count += uint64(n)
	return n, e
}
func (d *DB) tableGet(t *table, key []byte) (record, bool, error) {
	if bytes.Compare(key, t.desc.Min) < 0 || bytes.Compare(key, t.desc.Max) > 0 {
		return record{}, false, nil
	}
	d.count.BloomChecks++
	if !t.index.Bloom.mayContain(key) {
		d.count.BloomSkips++
		return record{}, false, nil
	}
	i := sort.Search(len(t.index.Blocks), func(i int) bool { return bytes.Compare(t.index.Blocks[i].First, key) > 0 }) - 1
	if i < 0 {
		return record{}, false, nil
	}
	b := t.index.Blocks[i]
	d.count.BlockReads++
	rd := countedReader{t.file, &d.count.ReadBytes}
	for o := b.Offset; o < b.Offset+b.Length; {
		r, next, e := readRecord(rd, o, b.Offset+b.Length)
		if e != nil {
			return record{}, false, corruption("table record %d at %d", t.desc.ID, o)
		}
		c := bytes.Compare(r.key, key)
		if c == 0 {
			return r, true, nil
		}
		if c > 0 {
			break
		}
		o = next
	}
	return record{}, false, nil
}

func strictJSON(b []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return ErrCorrupt
	}
	return nil
}
