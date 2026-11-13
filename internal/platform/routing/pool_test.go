package routing

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistryRejectsInvalidAndStaleUpdates(t *testing.T) {
	for _, r := range []Registry{{Revision: 0}, {Revision: 1, Endpoints: []string{"127.0.0.1:0"}}, {Revision: 1, Endpoints: []string{"http://localhost:90"}}, {Revision: 1, Endpoints: []string{"localhost:90", "localhost:90"}}, {Revision: 1, Endpoints: make([]string, 17)}} {
		if validate(r) == nil {
			t.Fatalf("accepted invalid registry: %+v", r)
		}
	}
	p, e := NewLocal(Registry{Revision: 2, Endpoints: []string{"127.0.0.1:1"}})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if p.Apply(Registry{Revision: 1}) == nil || p.Apply(Registry{Revision: 2, Endpoints: []string{"127.0.0.1:2"}}) == nil {
		t.Fatal("accepted stale/conflicting revision")
	}
	if e = p.Apply(Registry{Revision: 3, Endpoints: []string{}}); e != nil {
		t.Fatal(e)
	}
	if e = p.Apply(Registry{Revision: 3, Endpoints: []string{}}); e != nil {
		t.Fatal("empty snapshot not repeatable:", e)
	}
	path := filepath.Join(t.TempDir(), "registry.json")
	if e = os.WriteFile(path, []byte(strings.Repeat("x", 4097)), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadRegistry(path); e == nil {
		t.Fatal("accepted oversized registry")
	}
	for _, raw := range []string{`{"revision":4}`, `{"revision":4,"endpoints":null}`, `{"revision":4,"endpoints":[],"unknown":true}`, `{"revision":4,"endpoints":[]} {}`} {
		if e = os.WriteFile(path, []byte(raw), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e = ReadRegistry(path); e == nil {
			t.Fatalf("accepted malformed withdrawal: %s", raw)
		}
	}
	if e = os.WriteFile(path, []byte(`{"revision":4,"endpoints":[]}`), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadRegistry(path); e != nil {
		t.Fatal("explicit withdrawal rejected:", e)
	}
}
