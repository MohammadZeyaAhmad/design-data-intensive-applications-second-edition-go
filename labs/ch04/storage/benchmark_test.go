package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkWarmPointRead excludes fixture construction and checks the returned
// value inside the timed loop. Data and process caches are deliberately warm.
func BenchmarkWarmPointRead(b *testing.B) {
	for _, engine := range []string{"lsm-tiered", "lsm-leveled", "btree", "bbolt"} {
		b.Run(engine, func(b *testing.B) {
			parent := os.Getenv("ATLAS_BENCH_WORKDIR")
			if parent != "" {
				if e := os.MkdirAll(parent, 0700); e != nil {
					b.Fatal(e)
				}
			}
			dir, e := os.MkdirTemp(parent, "atlas-go-bench-")
			if e != nil {
				b.Fatal(e)
			}
			b.Cleanup(func() { os.RemoveAll(dir) })
			path := filepath.Join(dir, "db")
			if e = os.Mkdir(path, 0700); e != nil {
				b.Fatal(e)
			}
			db, e := openStore(path, engine)
			if e != nil {
				b.Fatal(e)
			}
			b.Cleanup(func() { db.Close() })
			keys, values := make([][]byte, 512), make([][]byte, 512)
			for i := range keys {
				keys[i] = key(i, false)
				values[i] = value(i, 1)
				if e = db.Put(keys[i], values[i]); e != nil {
					b.Fatal(e)
				}
			}
			if l, ok := db.(interface{ Flush() error }); ok {
				if e = l.Flush(); e != nil {
					b.Fatal(e)
				}
			}
			for _, k := range keys {
				if _, e = db.Get(k); e != nil {
					b.Fatal(e)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := i % len(keys)
				v, e := db.Get(keys[j])
				if e != nil || !bytes.Equal(v, values[j]) {
					b.Fatal("point read differs", e)
				}
			}
		})
	}
}
