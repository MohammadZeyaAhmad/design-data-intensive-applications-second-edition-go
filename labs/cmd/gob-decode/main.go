// gob-decode is a one-record worker. The parent supplies bounded bytes and
// enforces wall time; Linux limits fence the native gob decoder's allocations.
package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"os"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

func main() {
	if limits() != nil {
		os.Exit(2)
	}
	b, e := io.ReadAll(io.LimitReader(os.Stdin, model.MaxWire+2))
	if e != nil || len(b) < 2 || len(b) > model.MaxWire+1 {
		os.Exit(3)
	}
	d := gob.NewDecoder(bytes.NewReader(b[1:]))
	var r model.Record
	if b[0] == 1 {
		var v model.GobV1
		e = d.Decode(&v)
		r = v.Record()
	} else if b[0] == 2 {
		var v model.GobV2
		e = d.Decode(&v)
		r = v.Record()
	} else {
		os.Exit(3)
	}
	if e != nil || model.Validate(r) != nil {
		os.Exit(4)
	}
	var extra model.GobV1
	if d.Decode(&extra) != io.EOF {
		os.Exit(4)
	}
	if json.NewEncoder(os.Stdout).Encode(r) != nil {
		os.Exit(5)
	}
}
