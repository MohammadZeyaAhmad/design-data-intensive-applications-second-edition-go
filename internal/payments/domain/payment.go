package domain

import (
	"errors"
	"regexp"
)

var ErrInvalid = errors.New("invalid payment")
var currency = regexp.MustCompile(`^[A-Z]{3}$`)
var tenant = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var id = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func Valid(tenantID, paymentID string, amount int64, code, method string) bool {
	return tenant.MatchString(tenantID) && id.MatchString(paymentID) && amount > 0 && amount <= 9_000_000_000_000_000 && currency.MatchString(code) && ValidMethod(method)
}
func ValidMethod(s string) bool {
	return s == "test-ok" || s == "test-decline" || s == "test-lost-response" || s == "test-slow" || s == "test-refund-lost-response"
}
func Closed(status string) bool {
	return status == "refunded" || status == "voided" || status == "declined"
}
