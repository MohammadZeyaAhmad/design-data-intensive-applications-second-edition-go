package graphql

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Long int64

func (Long) ImplementsGraphQLType(name string) bool { return name == "Long" }
func (l *Long) UnmarshalGraphQL(v any) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("Long requires a decimal string")
	}
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil || strconv.FormatInt(n, 10) != s {
		return fmt.Errorf("invalid Long")
	}
	*l = Long(n)
	return nil
}
func (l Long) MarshalJSON() ([]byte, error) { return json.Marshal(strconv.FormatInt(int64(l), 10)) }

var tenantID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var skuID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var orderID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type cursorCodec struct{ key []byte }

func (c cursorCodec) encode(tenant, id string) string {
	body := []byte("1|order-projections|" + tenant + "|" + id)
	mac := hmac.New(sha256.New, c.key)
	mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (c cursorCodec) decode(tenant, value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if len(value) > 512 {
		return "", badInput()
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return "", badInput()
	}
	body, e := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if e != nil {
		return "", badInput()
	}
	signature, e := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if e != nil {
		return "", badInput()
	}
	mac := hmac.New(sha256.New, c.key)
	mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return "", badInput()
	}
	fields := strings.Split(string(body), "|")
	if len(fields) != 4 || fields[0] != "1" || fields[1] != "order-projections" || fields[2] != tenant || !orderID.MatchString(fields[3]) {
		return "", badInput()
	}
	return fields[3], nil
}

type publicError struct{ code, message string }

func (e *publicError) Error() string              { return e.message }
func (e *publicError) Extensions() map[string]any { return map[string]any{"code": e.code} }
func badInput() error                             { return &publicError{"BAD_USER_INPUT", "invalid read argument"} }
func unavailable() error                          { return &publicError{"UNAVAILABLE", "read unavailable"} }
func forbidden() error                            { return &publicError{"FORBIDDEN", "field access denied"} }
