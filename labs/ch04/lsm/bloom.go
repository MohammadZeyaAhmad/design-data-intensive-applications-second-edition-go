package lsm

import "encoding/binary"

// Bloom uses two stable 64-bit hashes to derive seven probes at ten bits/key.
// Tombstones are members too: filtering them out would resurrect older values.
type bloom struct {
	Bits   []byte `json:"bits"`
	Probes uint8  `json:"probes"`
}

func newBloom(n int) bloom {
	bits := (n*10 + 7) / 8
	if bits < 8 {
		bits = 8
	}
	return bloom{make([]byte, bits), 7}
}
func hashes(k []byte) (uint64, uint64) {
	a, b := uint64(14695981039346656037), uint64(0x9e3779b97f4a7c15)
	for _, c := range k {
		a = (a ^ uint64(c)) * 1099511628211
		b ^= uint64(c) + 0x9e3779b97f4a7c15 + (b << 6) + (b >> 2)
	}
	// Mix both halves; odd stride visits all positions for power-of-two filters.
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], a)
	for _, c := range buf {
		b = (b ^ uint64(c)) * 0x100000001b3
	}
	return a, b | 1
}
func (b bloom) add(k []byte) {
	a, h := hashes(k)
	m := uint64(len(b.Bits) * 8)
	for i := uint8(0); i < b.Probes; i++ {
		p := (a + uint64(i)*h) % m
		b.Bits[p/8] |= 1 << uint(p%8)
	}
}
func (b bloom) mayContain(k []byte) bool {
	a, h := hashes(k)
	m := uint64(len(b.Bits) * 8)
	for i := uint8(0); i < b.Probes; i++ {
		p := (a + uint64(i)*h) % m
		if b.Bits[p/8]&(1<<uint(p%8)) == 0 {
			return false
		}
	}
	return true
}
