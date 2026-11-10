package btree

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sort"
)

const logHeader = 32
const maxLogBytes = logHeader + 16 + maxDirtyPages*(PageSize+8)

var logMagic = []byte("ABTWAL01")
var errTail = io.ErrUnexpectedEOF

type redo struct {
	id         [16]byte
	generation uint64
	meta       metadata
	images     map[uint64][]byte
}

func encodeRedo(r redo) ([]byte, error) {
	if r.generation == 0 || len(r.images) < 1 || len(r.images) > maxDirtyPages {
		return nil, ErrLimit
	}
	ids := make([]uint64, 0, len(r.images))
	for id := range r.images {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	body := append([]byte(nil), r.id[:]...)
	for _, id := range ids {
		b := r.images[id]
		if len(b) != PageSize {
			return nil, corrupt("redo image size")
		}
		body = binary.LittleEndian.AppendUint64(body, id)
		body = append(body, b...)
	}
	b := make([]byte, logHeader)
	copy(b, logMagic)
	binary.LittleEndian.PutUint32(b[8:], uint32(len(body)))
	binary.LittleEndian.PutUint32(b[12:], uint32(len(ids)))
	binary.LittleEndian.PutUint64(b[16:], r.generation)
	binary.LittleEndian.PutUint32(b[24:], crc32.Checksum(body, crcTable))
	binary.LittleEndian.PutUint32(b[28:], crc32.Checksum(b[:28], crcTable))
	return append(b, body...), nil
}
func decodeRedo(b []byte) (redo, error) {
	var r redo
	if len(b) == 0 {
		return r, io.EOF
	}
	if len(b) < logHeader {
		return r, errTail
	}
	if !bytes.Equal(b[:8], logMagic) || crc32.Checksum(b[:28], crcTable) != binary.LittleEndian.Uint32(b[28:]) {
		return r, corrupt("redo header")
	}
	size, count := uint64(binary.LittleEndian.Uint32(b[8:])), uint64(binary.LittleEndian.Uint32(b[12:]))
	r.generation = binary.LittleEndian.Uint64(b[16:])
	if r.generation == 0 || count == 0 || count > maxDirtyPages || size != 16+count*(PageSize+8) {
		return r, corrupt("redo bounds")
	}
	if uint64(len(b)) < size+logHeader {
		return r, errTail
	}
	if uint64(len(b)) != size+logHeader {
		return r, corrupt("redo trailing bytes")
	}
	p := b[logHeader:]
	if crc32.Checksum(p, crcTable) != binary.LittleEndian.Uint32(b[24:]) {
		return r, corrupt("redo checksum")
	}
	copy(r.id[:], p[:16])
	p = p[16:]
	r.images = map[uint64][]byte{}
	var previous uint64
	for i := uint64(0); i < count; i++ {
		id := binary.LittleEndian.Uint64(p)
		p = p[8:]
		if id >= MaxPages || r.images[id] != nil || i == 0 && id != 0 || i > 0 && id <= previous {
			return r, corrupt("redo page identity")
		}
		r.images[id] = clone(p[:PageSize])
		previous = id
		p = p[PageSize:]
	}
	m, e := decodeMeta(r.images[0])
	if e != nil {
		return r, e
	}
	r.meta = m
	if m.ID != r.id || m.Generation != r.generation {
		return r, corrupt("redo metadata identity")
	}
	for id, b := range r.images {
		if id == 0 {
			continue
		}
		n, e := decodePage(b, id, m.Order)
		if e != nil {
			return r, e
		}
		if id >= m.Next || n.lsn != r.generation {
			return r, corrupt("redo page generation")
		}
	}
	return r, nil
}
