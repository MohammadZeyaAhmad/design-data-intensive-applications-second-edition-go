package contracts

import "testing"

func TestAsyncAPICommandAdmission(t *testing.T) {
	valid := []byte(`{"format":1,"writer_revision":2,"id":"stock-0","key":{"tenant":"acme","sku":"BOOK"},"kind":"receive.v1","data":{"quantity":10,"note":"retain me"}}`)
	if e := Validate("Command", valid); e != nil {
		t.Fatal(e)
	}
	for _, raw := range []string{`{"format":1,"writer_revision":9,"id":"id","key":{"tenant":"acme","sku":"BOOK"},"kind":"receive.v1","data":{"quantity":1}}`, `{"format":1,"writer_revision":2,"id":"id","key":{"tenant":"acme","sku":"BOOK"},"kind":"receive.v1","data":{"quantity":0}}`} {
		if Validate("Command", []byte(raw)) == nil {
			t.Fatal("contract accepted unsupported message")
		}
	}
}
