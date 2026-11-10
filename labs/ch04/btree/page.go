package btree

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var pageMagic = []byte("ABTPG001")
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func wrapPage(id, lsn, next uint64, kind byte, count int, payload []byte) ([]byte, error) {
	if id >= MaxPages || count < 0 || count > 8 || len(payload) > pageCRC-pageHeader {
		return nil, ErrLimit
	}
	b := make([]byte, PageSize)
	copy(b, pageMagic)
	binary.LittleEndian.PutUint64(b[8:], id)
	binary.LittleEndian.PutUint64(b[16:], lsn)
	b[24] = kind
	binary.LittleEndian.PutUint16(b[28:], uint16(count))
	binary.LittleEndian.PutUint16(b[30:], uint16(len(payload)))
	binary.LittleEndian.PutUint64(b[32:], next)
	copy(b[pageHeader:], payload)
	binary.LittleEndian.PutUint32(b[pageCRC:], crc32.Checksum(b[:pageCRC], crcTable))
	return b, nil
}
func unwrapPage(b []byte, id uint64) (kind byte, lsn, next uint64, count int, payload []byte, err error) {
	if len(b) != PageSize || !bytes.Equal(b[:8], pageMagic) || binary.LittleEndian.Uint64(b[8:]) != id || crc32.Checksum(b[:pageCRC], crcTable) != binary.LittleEndian.Uint32(b[pageCRC:]) {
		err = corrupt("page %d checksum, identity or version", id)
		return
	}
	if b[25] != 0 || b[26] != 0 || b[27] != 0 {
		err = corrupt("page flags")
		return
	}
	kind = b[24]
	lsn = binary.LittleEndian.Uint64(b[16:])
	next = binary.LittleEndian.Uint64(b[32:])
	count = int(binary.LittleEndian.Uint16(b[28:]))
	size := int(binary.LittleEndian.Uint16(b[30:]))
	if count > 8 || size > pageCRC-pageHeader || next >= MaxPages || next != 0 && next == id {
		err = corrupt("page bounds")
		return
	}
	payload = b[pageHeader : pageHeader+size]
	for _, v := range b[pageHeader+size : pageCRC] {
		if v != 0 {
			err = corrupt("page padding")
			return
		}
	}
	return
}
func encodePage(n *page, order int) ([]byte, error) {
	if n.id == 0 || !validOrder(order) || len(n.keys) > order {
		return nil, corrupt("page identity or capacity")
	}
	var p []byte
	switch n.kind {
	case kindLeaf:
		if len(n.values) != len(n.keys) || len(n.children) != 0 {
			return nil, corrupt("leaf shape")
		}
		for i, k := range n.keys {
			v := n.values[i]
			if !validKey(k) || len(v) > MaxValueBytes || i > 0 && bytes.Compare(n.keys[i-1], k) >= 0 {
				return nil, corrupt("leaf key/value")
			}
			p = binary.LittleEndian.AppendUint16(p, uint16(len(k)))
			p = binary.LittleEndian.AppendUint16(p, uint16(len(v)))
			p = append(p, k...)
			p = append(p, v...)
		}
	case kindBranch:
		if len(n.keys) == 0 || len(n.children) != len(n.keys)+1 || len(n.values) != 0 || n.next != 0 {
			return nil, corrupt("branch shape")
		}
		for _, id := range n.children {
			if id == 0 || id >= MaxPages || id == n.id {
				return nil, corrupt("child pointer")
			}
		}
		p = binary.LittleEndian.AppendUint64(p, n.children[0])
		for i, k := range n.keys {
			if !validKey(k) || i > 0 && bytes.Compare(n.keys[i-1], k) >= 0 {
				return nil, corrupt("separator order")
			}
			p = binary.LittleEndian.AppendUint16(p, uint16(len(k)))
			p = append(p, k...)
			p = binary.LittleEndian.AppendUint64(p, n.children[i+1])
		}
	case kindFree:
		if len(n.keys) != 0 || len(n.children) != 0 || len(n.values) != 0 {
			return nil, corrupt("free page shape")
		}
	default:
		return nil, corrupt("page type")
	}
	return wrapPage(n.id, n.lsn, n.next, n.kind, len(n.keys), p)
}
func decodePage(b []byte, id uint64, order int) (*page, error) {
	kind, lsn, next, count, p, e := unwrapPage(b, id)
	if e != nil {
		return nil, e
	}
	if id == 0 || !validOrder(order) || count > order {
		return nil, corrupt("node identity or capacity")
	}
	n := &page{id: id, lsn: lsn, next: next, kind: kind}
	readKey := func() ([]byte, error) {
		if len(p) < 2 {
			return nil, corrupt("short key length")
		}
		size := int(binary.LittleEndian.Uint16(p))
		p = p[2:]
		if size == 0 || size > MaxKeyBytes || size > len(p) {
			return nil, corrupt("key length")
		}
		key := clone(p[:size])
		p = p[size:]
		return key, nil
	}
	switch kind {
	case kindLeaf:
		for i := 0; i < count; i++ {
			if len(p) < 4 {
				return nil, corrupt("short leaf slot")
			}
			k, v := int(binary.LittleEndian.Uint16(p)), int(binary.LittleEndian.Uint16(p[2:]))
			p = p[4:]
			if k == 0 || k > MaxKeyBytes || v > MaxValueBytes || k+v > len(p) {
				return nil, corrupt("leaf slot bounds")
			}
			n.keys = append(n.keys, clone(p[:k]))
			n.values = append(n.values, clone(p[k:k+v]))
			p = p[k+v:]
		}
	case kindBranch:
		if count == 0 || len(p) < 8 || next != 0 {
			return nil, corrupt("short branch")
		}
		n.children = append(n.children, binary.LittleEndian.Uint64(p))
		p = p[8:]
		for i := 0; i < count; i++ {
			k, e := readKey()
			if e != nil {
				return nil, e
			}
			if len(p) < 8 {
				return nil, corrupt("short child")
			}
			n.keys = append(n.keys, k)
			n.children = append(n.children, binary.LittleEndian.Uint64(p))
			p = p[8:]
		}
		for _, child := range n.children {
			if child == 0 || child >= MaxPages || child == id {
				return nil, corrupt("child bounds")
			}
		}
	case kindFree:
		if count != 0 {
			return nil, corrupt("free page entries")
		}
	default:
		return nil, corrupt("unknown page type")
	}
	if len(p) != 0 {
		return nil, corrupt("page trailing payload")
	}
	for i := 1; i < len(n.keys); i++ {
		if bytes.Compare(n.keys[i-1], n.keys[i]) >= 0 {
			return nil, corrupt("page key order")
		}
	}
	return n, nil
}
func validMeta(m metadata) error {
	if m.ID == [16]byte{} || !validOrder(m.Order) || m.Height < 1 || m.Height > 16 || m.Next < 2 || m.Next > MaxPages || m.Root == 0 || m.Root >= m.Next || m.Free >= m.Next || m.FreeCount > m.Next-2 || m.Keys > (m.Next-1)*uint64(m.Order) {
		return corrupt("metadata bounds")
	}
	return nil
}
func encodeMeta(m metadata) ([]byte, error) {
	if e := validMeta(m); e != nil {
		return nil, e
	}
	p := append([]byte(nil), m.ID[:]...)
	for _, v := range []uint64{m.Root, m.Next, m.Free, m.FreeCount, m.Keys} {
		p = binary.LittleEndian.AppendUint64(p, v)
	}
	p = append(p, byte(m.Order), byte(m.Height))
	return wrapPage(0, m.Generation, 0, kindMeta, 0, p)
}
func decodeMeta(b []byte) (metadata, error) {
	var m metadata
	kind, lsn, next, count, p, e := unwrapPage(b, 0)
	if e != nil {
		return m, e
	}
	if kind != kindMeta || count != 0 || next != 0 || len(p) != 58 {
		return m, corrupt("metadata shape")
	}
	copy(m.ID[:], p[:16])
	m.Generation = lsn
	m.Root = binary.LittleEndian.Uint64(p[16:])
	m.Next = binary.LittleEndian.Uint64(p[24:])
	m.Free = binary.LittleEndian.Uint64(p[32:])
	m.FreeCount = binary.LittleEndian.Uint64(p[40:])
	m.Keys = binary.LittleEndian.Uint64(p[48:])
	m.Order = int(p[56])
	m.Height = int(p[57])
	return m, validMeta(m)
}
