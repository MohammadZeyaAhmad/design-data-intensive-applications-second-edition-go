package rdf

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTermsInterchangeAndIdentity(t *testing.T) {
	terms := []Term{String("quote \" slash \\\n你好 😀 <xml>"), Lang("colour", "EN-gb"), Typed("01", XSD+"integer"), Integer(1), Boolean(true), URI("urn:atlas:tenant:acme:node:wh-000")}
	s := New()
	for _, term := range terms {
		if _, e := s.Add(Triple{URI("urn:s"), URI("urn:p"), term}); e != nil {
			t.Fatal(e)
		}
	}
	if terms[2] == terms[3] || Lang("x", "EN") != Lang("x", "en") {
		t.Fatal("term identity lost")
	}
	parsed, e := Parse(strings.NewReader(s.NTriples()), "document-a")
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(s.Triples(), parsed.Triples()) {
		t.Fatal("roundtrip changed terms")
	}
	unicode, e := Parse(strings.NewReader(`<urn:s> <urn:p> "\U0001F600\u0041\'" .`), "unicode")
	if e != nil {
		t.Fatal(e)
	}
	if unicode.Triples()[0].Object != String("😀A'") {
		t.Fatal(unicode.Triples())
	}
	for _, bad := range []string{`<urn:s> <urn:p> "\uD800" .`, `<urn:s> <urn:p> "\/" .`, `<urn:s> <urn:p> "\U00110000" .`, `<urn:s> <urn:p> "x"; <urn:q> "y" .`, `"s" <urn:p> "o" .`, `<urn:s> _:p "o" .`, `<urn:s> <urn:p> "x"@ .`} {
		if _, e := Parse(strings.NewReader(bad), "bad"); e == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestBlankLabelsAreDocumentLocal(t *testing.T) {
	source := `_:x <urn:knows> _:x .
 _:x <urn:name> "same" .`
	a, e := Parse(strings.NewReader(source), "a")
	if e != nil {
		t.Fatal(e)
	}
	b, e := Parse(strings.NewReader(source), "b")
	if e != nil {
		t.Fatal(e)
	}
	if a.Triples()[0].Subject == b.Triples()[0].Subject {
		t.Fatal("blank labels became global IDs")
	}
	knows := URI("urn:knows")
	r, _ := a.Match(nil, &knows, nil)
	if len(r) != 1 || r[0].Subject != r[0].Object {
		t.Fatal("document co-reference lost")
	}
}

func TestIndexesAgreeWithFullScanAndSetSemantics(t *testing.T) {
	s := New()
	for i := 0; i < 120; i++ {
		_, e := s.Add(Triple{URI(fmt.Sprintf("urn:s:%d", i%12)), URI(fmt.Sprintf("urn:p:%d", i%3)), Integer(int64(i % 7))})
		if e != nil {
			t.Fatal(e)
		}
	}
	all := s.Triples()
	for _, triple := range all {
		if added, e := s.Add(triple); e != nil || added {
			t.Fatal("duplicate changed RDF set")
		}
		for mask := 0; mask < 8; mask++ {
			var subject, predicate, object *Term
			if mask&1 != 0 {
				subject = &triple.Subject
			}
			if mask&2 != 0 {
				predicate = &triple.Predicate
			}
			if mask&4 != 0 {
				object = &triple.Object
			}
			got, candidates := s.Match(subject, predicate, object)
			want := []Triple{}
			for _, x := range all {
				if subject != nil && x.Subject != *subject || predicate != nil && x.Predicate != *predicate || object != nil && x.Object != *object {
					continue
				}
				want = append(want, x)
			}
			if !reflect.DeepEqual(got, want) || candidates > len(all) {
				t.Fatal("index mismatch")
			}
		}
	}
}

func TestURIInjectionAndTenantIdentity(t *testing.T) {
	for _, s := range []string{"relative", "urn:x> } INSERT {}", "urn:x\n", "urn:x\\u003e"} {
		if URI(s).Valid() {
			t.Fatal(s)
		}
	}
	a, _ := EntityURI("acme", "node", "same")
	b, _ := EntityURI("globex", "node", "same")
	if a == b {
		t.Fatal("tenant identity collided")
	}
	if _, e := NodeID(b, "acme"); e == nil {
		t.Fatal("cross tenant URI accepted")
	}
	if _, e := EntityURI("acme", "node", "x> }"); e == nil {
		t.Fatal("ID injection accepted")
	}
}

func FuzzNTriplesRoundTrip(f *testing.F) {
	for _, s := range []string{"simple", "你好 😀", "quote\"\n\\", "<xml>", "\u0001"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 4096 || !String(s).Valid() {
			return
		}
		tr := Triple{URI("urn:s"), URI("urn:p"), String(s)}
		wire, e := tr.NT()
		if e != nil {
			t.Fatal(e)
		}
		store, e := Parse(strings.NewReader(wire), "fuzz")
		if e != nil {
			t.Fatal(e)
		}
		if got := store.Triples(); len(got) != 1 || got[0] != tr {
			t.Fatal("literal changed")
		}
	})
}
