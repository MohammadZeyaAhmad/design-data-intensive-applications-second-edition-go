package singleleader

import (
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveRetryIdentityAndRestore(t *testing.T) {
	d := t.TempDir()
	src := filepath.Join(t.TempDir(), "wal")
	name := "000000010000000000000001"
	if err := os.WriteFile(src, []byte("durable-wal"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := Archive(d, src, name); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(t.TempDir(), "recovered")
	if err := Restore(d, name, out); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	if string(b) != "durable-wal" {
		t.Fatal("restore mismatch")
	}
	if err := os.WriteFile(src, []byte("different"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Archive(d, src, name); err == nil {
		t.Fatal("conflicting overwrite accepted")
	}
	for _, name := range []string{"../escape", "", "00000001.history/escape", "lowercase"} {
		if Archive(d, src, name) == nil || Restore(d, name, out) == nil {
			t.Fatal("unsafe name admitted")
		}
	}
}
