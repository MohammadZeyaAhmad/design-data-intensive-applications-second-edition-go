package domain

import (
	"errors"
	"math"
	"regexp"
)

var ErrInvalid = errors.New("invalid product or quote")
var sku = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var currency = regexp.MustCompile(`^[A-Z]{3}$`)

const MaxTotal int64 = 9_000_000_000_000_000

type Product struct {
	SKU            string `json:"sku"`
	Name           string `json:"name"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	Currency       string `json:"currency"`
	Version        int64  `json:"version"`
}
type Line struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}

func ValidProduct(p Product) bool {
	return sku.MatchString(p.SKU) && len(p.Name) > 0 && len(p.Name) <= 200 && p.UnitPriceMinor > 0 && p.UnitPriceMinor <= 1_000_000_000_000 && currency.MatchString(p.Currency) && p.Version >= 0 && p.Version < math.MaxInt64
}
func ValidLines(lines []Line) bool {
	if len(lines) < 1 || len(lines) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, l := range lines {
		if !sku.MatchString(l.SKU) || l.Quantity < 1 || l.Quantity > 1_000_000_000 || seen[l.SKU] {
			return false
		}
		seen[l.SKU] = true
	}
	return true
}
func Add(total, price, quantity int64) (int64, error) {
	if price < 1 || quantity < 1 || price > MaxTotal/quantity {
		return 0, ErrInvalid
	}
	amount := price * quantity
	if total < 0 || total > MaxTotal-amount {
		return 0, ErrInvalid
	}
	return total + amount, nil
}
