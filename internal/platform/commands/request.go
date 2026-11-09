package commands

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
)

var ErrInvalid = errors.New("invalid command identity")
var ErrConflict = errors.New("command identity has another payload")
var tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

type Request struct {
	Tenant, Operation, Key string
	Hash                   [32]byte
}

func New(tenant, operation, key string, payload any) (Request, error) {
	if !tenantPattern.MatchString(tenant) || len(key) < 1 || len(key) > 128 {
		return Request{}, ErrInvalid
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return Request{}, ErrInvalid
		}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Request{}, err
	}
	return Request{tenant, operation, key, sha256.Sum256(b)}, nil
}
