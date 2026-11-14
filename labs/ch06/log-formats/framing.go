package logformats

import (
	"bytes"
	"encoding/binary"
)

// Preflight every count/length before the upstream parser allocates from wire
// fields. A one-megabyte frame alone does not bound a forged four-byte count.
type cursor struct {
	data []byte
	at   int
}

func (c *cursor) take(n int) ([]byte, error) {
	if n < 0 || n > len(c.data)-c.at {
		return nil, ErrProtocol
	}
	part := c.data[c.at : c.at+n]
	c.at += n
	return part, nil
}
func (c *cursor) byte() (byte, error) {
	b, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}
func (c *cursor) count16() (int, error) {
	b, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint16(b)), nil
}
func (c *cursor) count32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}
func (c *cursor) name() error {
	n := bytes.IndexByte(c.data[c.at:], 0)
	if n < 1 || n > 48 {
		return ErrProtocol
	}
	_, err := c.take(n + 1)
	return err
}
func (c *cursor) tuple() error {
	n, err := c.count16()
	if err != nil {
		return err
	}
	if n < 1 || n > MaxColumns {
		return ErrLimit
	}
	for range n {
		kind, err := c.byte()
		if err != nil {
			return err
		}
		switch kind {
		case 'n', 'u':
		case 't':
			size, err := c.count32()
			if err != nil {
				return err
			}
			if size > MaxFrameBytes {
				return ErrLimit
			}
			if _, err = c.take(int(size)); err != nil {
				return err
			}
		default:
			return ErrProtocol
		}
	}
	return nil
}
func preflight(frame []byte) error {
	if len(frame) == 0 {
		return ErrProtocol
	}
	if len(frame) > MaxFrameBytes {
		return ErrLimit
	}
	c := cursor{data: frame, at: 1}
	switch frame[0] {
	case 'B':
		if _, err := c.take(20); err != nil {
			return err
		}
	case 'C':
		if _, err := c.take(25); err != nil {
			return err
		}
	case 'R':
		if _, err := c.take(4); err != nil {
			return err
		}
		if err := c.name(); err != nil {
			return err
		}
		if err := c.name(); err != nil {
			return err
		}
		if _, err := c.take(1); err != nil {
			return err
		}
		n, err := c.count16()
		if err != nil {
			return err
		}
		if n < 1 || n > MaxColumns {
			return ErrLimit
		}
		for range n {
			if _, err = c.take(1); err != nil {
				return err
			}
			if err = c.name(); err != nil {
				return err
			}
			if _, err = c.take(8); err != nil {
				return err
			}
		}
	case 'I':
		if _, err := c.take(4); err != nil {
			return err
		}
		tag, err := c.byte()
		if err != nil || tag != 'N' {
			return ErrProtocol
		}
		if err = c.tuple(); err != nil {
			return err
		}
	case 'U':
		if _, err := c.take(4); err != nil {
			return err
		}
		tag, err := c.byte()
		if err != nil {
			return err
		}
		if tag == 'K' || tag == 'O' {
			if err = c.tuple(); err != nil {
				return err
			}
			tag, err = c.byte()
			if err != nil {
				return err
			}
		}
		if tag != 'N' {
			return ErrProtocol
		}
		if err = c.tuple(); err != nil {
			return err
		}
	case 'D':
		if _, err := c.take(4); err != nil {
			return err
		}
		tag, err := c.byte()
		if err != nil || (tag != 'K' && tag != 'O') {
			return ErrProtocol
		}
		if err = c.tuple(); err != nil {
			return err
		}
	case 'T':
		n, err := c.count32()
		if err != nil {
			return err
		}
		if n < 1 || n > MaxRelations {
			return ErrLimit
		}
		flags, err := c.byte()
		if err != nil || flags != 0 {
			return ErrProtocol
		}
		if _, err = c.take(int(n) * 4); err != nil {
			return err
		}
	default:
		return ErrProtocol
	}
	if c.at != len(frame) {
		return ErrProtocol
	}
	return nil
}
