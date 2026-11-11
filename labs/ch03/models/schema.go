package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

// DecodeWeight migrates the explicitly supported legacy string to the current
// integer field. Absent/null/zero, unknown versions and invalid values differ.
func DecodeWeight(body []byte) (grams *int64, legacy bool, err error) {
	if len(body) > 4096 {
		return nil, false, ErrInvalid
	}
	var d struct {
		Schema int             `json:"schema"`
		Weight json.RawMessage `json:"weight_grams"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, false, ErrInvalid
	}
	if d.Schema != 1 && d.Schema != 2 {
		return nil, false, ErrInvalid
	}
	legacy = d.Schema == 1
	if len(d.Weight) == 0 || bytes.Equal(d.Weight, []byte("null")) {
		return nil, legacy, nil
	}
	var n int64
	if legacy {
		var s string
		if json.Unmarshal(d.Weight, &s) != nil {
			return nil, true, ErrInvalid
		}
		n, err = strconv.ParseInt(s, 10, 64)
	} else {
		err = json.Unmarshal(d.Weight, &n)
	}
	if err != nil || n < 0 || n > 1e9 {
		return nil, legacy, fmt.Errorf("weight schema: %w", ErrInvalid)
	}
	return &n, legacy, nil
}
