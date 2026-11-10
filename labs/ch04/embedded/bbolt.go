// Package embedded adapts a mature embedded engine to the lab's key/value API.
// bbolt is a copy-on-write B+ tree, making its layout difference explicit.
package embedded

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"go.etcd.io/bbolt"
)

var bucket = []byte("warehouse-snapshots-v1")

type Bolt struct {
	db   *bbolt.DB
	path string
}

func OpenBolt(path string) (*Bolt, error) {
	db, e := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second, NoSync: false})
	if e != nil {
		return nil, e
	}
	if e = db.Update(func(tx *bbolt.Tx) error { _, e := tx.CreateBucketIfNotExists(bucket); return e }); e != nil {
		db.Close()
		return nil, e
	}
	// A synced file also needs a durable parent directory entry at creation.
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		db.Close()
		return nil, e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Bolt{db, path}, nil
}
func (b *Bolt) Put(k, v []byte) error {
	if len(k) == 0 || len(k) > kv.MaxKeyBytes || len(v) > kv.MaxValueBytes {
		return kv.ErrInvalid
	}
	return b.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucket).Put(k, v) })
}
func (b *Bolt) Delete(k []byte) error {
	if len(k) == 0 || len(k) > kv.MaxKeyBytes {
		return kv.ErrInvalid
	}
	return b.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucket).Delete(k) })
}
func (b *Bolt) Get(k []byte) ([]byte, error) {
	if len(k) == 0 || len(k) > kv.MaxKeyBytes {
		return nil, kv.ErrInvalid
	}
	var value []byte
	e := b.db.View(func(tx *bbolt.Tx) error {
		found, v := tx.Bucket(bucket).Cursor().Seek(k)
		if !bytes.Equal(found, k) {
			return kv.ErrNotFound
		}
		value = append([]byte(nil), v...)
		return nil
	})
	return value, e
}
func (b *Bolt) Scan(start, end []byte, limit int) ([]kv.Pair, error) {
	if len(start) > kv.MaxKeyBytes || len(end) > kv.MaxKeyBytes+1 || limit < 1 || limit > 65536 || end != nil && bytes.Compare(start, end) > 0 {
		return nil, kv.ErrInvalid
	}
	var rows []kv.Pair
	e := b.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		for k, v := c.Seek(start); k != nil && len(rows) < limit; k, v = c.Next() {
			if end != nil && bytes.Compare(k, end) >= 0 {
				break
			}
			rows = append(rows, kv.Pair{Key: append([]byte(nil), k...), Value: append([]byte(nil), v...)})
		}
		return nil
	})
	return rows, e
}
func (b *Bolt) DiskBytes() (int64, error) {
	s, e := os.Stat(b.path)
	if e != nil {
		return 0, e
	}
	return s.Size(), nil
}
func (b *Bolt) Close() error { return b.db.Close() }
