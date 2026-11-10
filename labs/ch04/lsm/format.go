package lsm

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)
var frameMagic = [4]byte{'A', 'K', 'V', 1}
var errTail = io.ErrUnexpectedEOF

func encodeRecord(r record) []byte {
	b := make([]byte, frameHeader+17+len(r.key)+len(r.value))
	copy(b, frameMagic[:])
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-frameHeader))
	p := b[frameHeader:]
	binary.LittleEndian.PutUint64(p, r.seq)
	if r.deleted {
		p[8] = 1
	}
	binary.LittleEndian.PutUint32(p[9:], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(p[13:], uint32(len(r.value)))
	copy(p[17:], r.key)
	copy(p[17+len(r.key):], r.value)
	binary.LittleEndian.PutUint32(b[8:], crc32.Checksum(p, crcTable))
	binary.LittleEndian.PutUint32(b[12:], crc32.Checksum(b[:12], crcTable))
	return b
}
func decodeBody(b []byte) (record, error) {
	var r record
	if len(b) < 17 {
		return r, corruption("short record")
	}
	r.seq = binary.LittleEndian.Uint64(b)
	r.deleted = b[8] == 1
	k, v := uint64(binary.LittleEndian.Uint32(b[9:])), uint64(binary.LittleEndian.Uint32(b[13:]))
	if r.seq == 0 || b[8] > 1 || k == 0 || k > MaxKeyBytes || v > MaxValueBytes || k+v+17 != uint64(len(b)) || r.deleted && v != 0 {
		return r, corruption("invalid record bounds or type")
	}
	r.key = clone(b[17 : 17+k])
	r.value = clone(b[17+k:])
	return r, nil
}
func readRecord(f io.ReaderAt, offset, end int64) (record, int64, error) {
	var r record
	if offset == end {
		return r, offset, io.EOF
	}
	if offset < 0 || end-offset < frameHeader {
		return r, offset, errTail
	}
	h := make([]byte, frameHeader)
	if _, e := f.ReadAt(h, offset); e != nil {
		return r, offset, e
	}
	if string(h[:4]) != string(frameMagic[:]) || crc32.Checksum(h[:12], crcTable) != binary.LittleEndian.Uint32(h[12:]) {
		return r, offset, corruption("frame header at offset %d", offset)
	}
	n := int64(binary.LittleEndian.Uint32(h[4:]))
	if n < 17 || n > maxRecordBytes {
		return r, offset, corruption("frame length at offset %d", offset)
	}
	if n > end-offset-frameHeader {
		return r, offset, errTail
	}
	b := make([]byte, n)
	if _, e := f.ReadAt(b, offset+frameHeader); e != nil {
		return r, offset, e
	}
	if crc32.Checksum(b, crcTable) != binary.LittleEndian.Uint32(h[8:]) {
		return r, offset, corruption("frame checksum at offset %d", offset)
	}
	r, e := decodeBody(b)
	return r, offset + frameHeader + n, e
}
