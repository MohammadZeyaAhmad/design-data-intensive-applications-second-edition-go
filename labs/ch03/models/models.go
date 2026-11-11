// Package models compares bounded catalog representations over the same facts.
// bbolt persists snapshots and the query-shaped column-family key space; it is
// not a distributed wide-column database or an analytical column codec.
package models

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

var ErrInvalid = errors.New("invalid model fixture or query")
var ErrRecovery = errors.New("model store publication failed; close and reopen")

type Brand struct{ Tenant, ID, Name string }
type Variant struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}
type Product struct {
	Tenant     string            `json:"tenant"`
	SKU        string            `json:"sku"`
	Name       string            `json:"name"`
	BrandID    string            `json:"brand_id"`
	Price      int64             `json:"price"`
	Categories []string          `json:"categories"`
	Variants   []Variant         `json:"variants"`
	Attributes map[string]string `json:"attributes"`
}
type Customer struct {
	Tenant, ID, Name string
	Favorites        []string
}
type Fixture struct {
	Brands    []Brand
	Products  []Product
	Customers []Customer
}
type Document struct {
	Schema    int     `json:"schema"`
	Product   Product `json:"product"`
	BrandName string  `json:"brand_name"`
}
type flatRow struct {
	Tenant, Category, SKU, Name, BrandName, Variant string
	Price                                           int64
}
type Card struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Brand    string `json:"brand"`
	Price    int64  `json:"price"`
	Variants int    `json:"variants"`
}
type Query struct {
	Tenant, Category, Customer string
	MaxPrice                   int64
	Limit                      int
}
type Work struct {
	Rows    int `json:"rows_visited"`
	Lookups int `json:"reference_lookups"`
	Bytes   int `json:"logical_bytes_decoded"`
	Cells   int `json:"column_family_cells"`
}
type Result struct {
	Cards []Card `json:"cards"`
	Work  Work   `json:"work"`
}
type Store struct {
	mu     sync.Mutex
	disk   *bolt.DB
	data   Fixture
	docs   map[string][]byte
	flat   []flatRow
	wide   map[string][]byte
	closed bool
	bad    error
}

var state = []byte("state")
var families = []byte("families")

func key(tenant, id string) string             { return tenant + "\x00" + id }
func partition(tenant, category string) []byte { return []byte(tenant + "\x00" + category + "\x00") }
func cellKey(p Product, category, family string) []byte {
	k := partition(p.Tenant, category)
	k = binary.BigEndian.AppendUint64(k, uint64(p.Price))
	k = append(k, 0)
	k = append(k, p.SKU...)
	k = append(k, 0)
	return append(k, family...)
}
func FixtureFor(rows int) Fixture {
	f := Fixture{}
	for _, tenant := range []string{"acme", "globex"} {
		for i := 0; i < 8; i++ {
			f.Brands = append(f.Brands, Brand{tenant, fmt.Sprintf("brand-%d", i), fmt.Sprintf("Maker %d", i)})
		}
		for i := 0; i < 8; i++ {
			f.Customers = append(f.Customers, Customer{Tenant: tenant, ID: fmt.Sprintf("customer-%d", i), Name: fmt.Sprintf("Reader %d", i)})
		}
	}
	for i := 0; i < rows; i++ {
		tenant := "acme"
		if i%2 == 1 {
			tenant = "globex"
		}
		p := Product{Tenant: tenant, SKU: fmt.Sprintf("sku-%05d", i), Name: fmt.Sprintf("Device %05d", i), BrandID: fmt.Sprintf("brand-%d", i%8), Price: int64(100 + i*17%10000), Categories: []string{"all", fmt.Sprintf("category-%d", i%4)}, Attributes: map[string]string{"material": "recycled", "weight_grams": "120"}}
		for j := 0; j < 1+i%3; j++ {
			p.Variants = append(p.Variants, Variant{fmt.Sprintf("v-%d", j), fmt.Sprintf("Option %d", j)})
		}
		f.Products = append(f.Products, p)
		for j := range f.Customers {
			c := &f.Customers[j]
			if c.Tenant == tenant && i%8 == j%8 {
				c.Favorites = append(c.Favorites, p.SKU)
			}
		}
	}
	return f
}
func validName(s string) bool {
	return len(s) > 0 && len(s) <= 64 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func validate(f Fixture) error {
	if len(f.Products) > 4096 || len(f.Brands) > 128 || len(f.Customers) > 128 {
		return ErrInvalid
	}
	brands := map[string]bool{}
	products := map[string]bool{}
	expanded := 0
	for _, b := range f.Brands {
		k := key(b.Tenant, b.ID)
		if !validName(b.Tenant) || !validName(b.ID) || b.Name == "" || len(b.Name) > 200 || !utf8.ValidString(b.Name) || brands[k] {
			return ErrInvalid
		}
		brands[k] = true
	}
	for _, p := range f.Products {
		k := key(p.Tenant, p.SKU)
		if !validName(p.Tenant) || !validName(p.SKU) || products[k] || !brands[key(p.Tenant, p.BrandID)] || p.Price < 1 || p.Price > 1e12 || len(p.Categories) < 1 || len(p.Categories) > 16 || len(p.Variants) < 1 || len(p.Variants) > 32 || len(p.Name) == 0 || len(p.Name) > 256 || !utf8.ValidString(p.Name) || len(p.Attributes) > 32 {
			return ErrInvalid
		}
		products[k] = true
		expanded += len(p.Categories) * len(p.Variants)
		if expanded > 32768 {
			return ErrInvalid
		}
		seen := map[string]bool{}
		for _, c := range p.Categories {
			if !validName(c) || seen[c] {
				return ErrInvalid
			}
			seen[c] = true
		}
		seen = map[string]bool{}
		for _, v := range p.Variants {
			if !validName(v.Code) || seen[v.Code] || len(v.Label) == 0 || len(v.Label) > 100 || !utf8.ValidString(v.Label) {
				return ErrInvalid
			}
			seen[v.Code] = true
		}
		for k, v := range p.Attributes {
			if !validName(k) || len(v) > 256 || !utf8.ValidString(v) {
				return ErrInvalid
			}
		}
	}
	seen := map[string]bool{}
	for _, c := range f.Customers {
		k := key(c.Tenant, c.ID)
		if !validName(c.Tenant) || !validName(c.ID) || seen[k] || len(c.Name) > 200 || !utf8.ValidString(c.Name) || len(c.Favorites) > 4096 {
			return ErrInvalid
		}
		seen[k] = true
		favorites := map[string]bool{}
		for _, sku := range c.Favorites {
			if !products[key(c.Tenant, sku)] || favorites[sku] {
				return ErrInvalid
			}
			favorites[sku] = true
		}
	}
	encoded, e := json.Marshal(f)
	if e != nil || len(encoded) > 4<<20 {
		return ErrInvalid
	}
	return nil
}
func Open(path string, initial *Fixture) (*Store, error) {
	if initial != nil {
		if e := validate(*initial); e != nil {
			return nil, e
		}
	}
	d, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, e
	}
	s := &Store{disk: d}
	e = d.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state)
		if b == nil {
			if k, _ := tx.Cursor().First(); k != nil {
				return ErrInvalid
			}
			return nil
		}
		var snapshot struct {
			Data Fixture
			Docs map[string][]byte
			Flat []flatRow
		}
		body := b.Get([]byte("snapshot"))
		if len(body) > 64<<20 {
			return ErrInvalid
		}
		if e := json.Unmarshal(body, &snapshot); e != nil {
			return e
		}
		if e := validate(snapshot.Data); e != nil {
			return e
		}
		if len(snapshot.Docs) > 4096 || len(snapshot.Flat) > 32768 {
			return ErrInvalid
		}
		s.data, s.docs, s.flat = snapshot.Data, snapshot.Docs, snapshot.Flat
		s.wide = map[string][]byte{}
		w := tx.Bucket(families)
		if w == nil {
			return ErrInvalid
		}
		return w.ForEach(func(k, v []byte) error {
			if len(s.wide) >= 65536 || len(k) > 256 || len(v) > 65536 {
				return ErrInvalid
			}
			s.wide[string(k)] = append([]byte(nil), v...)
			return nil
		})
	})
	if e == nil && s.docs == nil {
		if initial == nil {
			e = ErrInvalid
		} else {
			b, _ := json.Marshal(initial)
			e = json.Unmarshal(b, &s.data)
			if e == nil {
				s.rebuild()
				e = s.persist()
			}
		}
	}
	if e != nil {
		d.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.disk.Close()
}
func (s *Store) rebuild() {
	s.docs = map[string][]byte{}
	s.flat = nil
	s.wide = map[string][]byte{}
	brands := map[string]string{}
	for _, b := range s.data.Brands {
		brands[key(b.Tenant, b.ID)] = b.Name
	}
	for _, p := range s.data.Products {
		brand := brands[key(p.Tenant, p.BrandID)]
		doc, _ := json.Marshal(Document{2, p, brand})
		s.docs[key(p.Tenant, p.SKU)] = doc
		for _, c := range p.Categories {
			for _, v := range p.Variants {
				s.flat = append(s.flat, flatRow{p.Tenant, c, p.SKU, p.Name, brand, v.Code, p.Price})
			}
			card, _ := json.Marshal(Card{p.SKU, p.Name, brand, p.Price, len(p.Variants)})
			s.wide[string(cellKey(p, c, "identity"))] = card
			options, _ := json.Marshal(p.Variants)
			s.wide[string(cellKey(p, c, "options"))] = options
		}
	}
}
func (s *Store) persist() error {
	if e := validate(s.data); e != nil {
		s.bad = errors.Join(ErrRecovery, e)
		return s.bad
	}
	body, e := json.Marshal(struct {
		Data Fixture
		Docs map[string][]byte
		Flat []flatRow
	}{s.data, s.docs, s.flat})
	if e != nil {
		s.bad = errors.Join(ErrRecovery, e)
		return s.bad
	}
	if len(body) > 64<<20 {
		s.bad = errors.Join(ErrRecovery, ErrInvalid)
		return s.bad
	}
	e = s.disk.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucketIfNotExists(state)
		if e != nil {
			return e
		}
		if e = b.Put([]byte("snapshot"), body); e != nil {
			return e
		}
		if tx.Bucket(families) != nil {
			if e = tx.DeleteBucket(families); e != nil {
				return e
			}
		}
		w, e := tx.CreateBucket(families)
		if e != nil {
			return e
		}
		for k, v := range s.wide {
			if e = w.Put([]byte(k), v); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		s.bad = errors.Join(ErrRecovery, e)
		return s.bad
	}
	return nil
}
func (s *Store) ready() error {
	if s.closed {
		return os.ErrClosed
	}
	return s.bad
}
func favorites(f Fixture, q Query) (map[string]bool, bool) {
	if q.Customer == "" {
		return nil, true
	}
	for _, c := range f.Customers {
		if c.Tenant == q.Tenant && c.ID == q.Customer {
			m := map[string]bool{}
			for _, sku := range c.Favorites {
				m[sku] = true
			}
			return m, true
		}
	}
	return nil, false
}
func eligible(p Product, q Query, fav map[string]bool) bool {
	if p.Tenant != q.Tenant || p.Price > q.MaxPrice || q.Customer != "" && !fav[p.SKU] {
		return false
	}
	for _, c := range p.Categories {
		if c == q.Category {
			return true
		}
	}
	return false
}
func ordered(r *Result, k int) {
	sort.Slice(r.Cards, func(i, j int) bool {
		a, b := r.Cards[i], r.Cards[j]
		return a.Price < b.Price || a.Price == b.Price && a.SKU < b.SKU
	})
	if len(r.Cards) > k {
		r.Cards = r.Cards[:k]
	}
	if r.Cards == nil {
		r.Cards = []Card{}
	}
}
func (s *Store) Query(mode string, q Query) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r Result
	if mode != "normalized" && mode != "document" && mode != "denormalized" && mode != "wide-column" {
		return r, ErrInvalid
	}
	if e := s.ready(); e != nil {
		return r, e
	}
	if !validName(q.Tenant) || !validName(q.Category) || q.Customer != "" && !validName(q.Customer) || q.MaxPrice < 0 || q.MaxPrice > 1e12 || q.Limit < 1 || q.Limit > 100 {
		return r, ErrInvalid
	}
	fav, ok := favorites(s.data, q)
	if !ok {
		r.Cards = []Card{}
		return r, nil
	}
	switch mode {
	case "normalized":
		for _, p := range s.data.Products {
			r.Work.Rows++
			if !eligible(p, q, fav) {
				continue
			}
			var brand string
			for _, b := range s.data.Brands {
				r.Work.Lookups++
				if b.Tenant == q.Tenant && b.ID == p.BrandID {
					brand = b.Name
					break
				}
			}
			r.Cards = append(r.Cards, Card{p.SKU, p.Name, brand, p.Price, len(p.Variants)})
		}
	case "document":
		for _, body := range s.docs {
			r.Work.Rows++
			r.Work.Bytes += len(body)
			var d Document
			if e := json.Unmarshal(body, &d); e != nil {
				return r, e
			}
			if eligible(d.Product, q, fav) {
				p := d.Product
				r.Cards = append(r.Cards, Card{p.SKU, p.Name, d.BrandName, p.Price, len(p.Variants)})
			}
		}
	case "denormalized":
		cards := map[string]Card{}
		for _, row := range s.flat {
			r.Work.Rows++
			if row.Tenant != q.Tenant || row.Category != q.Category || row.Price > q.MaxPrice || q.Customer != "" && !fav[row.SKU] {
				continue
			}
			c := cards[row.SKU]
			c.SKU, c.Name, c.Brand, c.Price = row.SKU, row.Name, row.BrandName, row.Price
			c.Variants++
			cards[row.SKU] = c
		}
		for _, c := range cards {
			r.Cards = append(r.Cards, c)
		}
	case "wide-column":
		prefix := partition(q.Tenant, q.Category)
		e := s.disk.View(func(tx *bolt.Tx) error {
			cursor := tx.Bucket(families).Cursor()
			for k, v := cursor.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, v = cursor.Next() {
				if len(k) < len(prefix)+9 {
					return ErrInvalid
				}
				price := int64(binary.BigEndian.Uint64(k[len(prefix):]))
				if price > q.MaxPrice {
					break
				}
				r.Work.Cells++
				if !strings.HasSuffix(string(k), "\x00identity") {
					continue
				}
				r.Work.Rows++
				r.Work.Bytes += len(v)
				var c Card
				if e := json.Unmarshal(v, &c); e != nil {
					return e
				}
				if q.Customer != "" && !fav[c.SKU] {
					continue
				}
				r.Cards = append(r.Cards, c)
				if len(r.Cards) == q.Limit {
					break
				}
			}
			return nil
		})
		if e != nil {
			return r, e
		}
	default:
		return r, ErrInvalid
	}
	ordered(&r, q.Limit)
	return r, nil
}

// RenamePartially is an intentional anomaly exercise: publish the normalized
// reference and update only one embedded copy. Repair must restore all copies.
func (s *Store) RenamePartially(tenant, id, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.ready(); e != nil {
		return e
	}
	if !validName(tenant) || !validName(id) || name == "" || len(name) > 200 || !utf8.ValidString(name) {
		return ErrInvalid
	}
	found := false
	for i := range s.data.Brands {
		b := &s.data.Brands[i]
		if b.Tenant == tenant && b.ID == id {
			b.Name = name
			found = true
		}
	}
	if !found {
		return ErrInvalid
	}
	keys := make([]string, 0, len(s.docs))
	for k := range s.docs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var d Document
		if e := json.Unmarshal(s.docs[k], &d); e != nil {
			return e
		}
		if d.Product.Tenant == tenant && d.Product.BrandID == id {
			d.BrandName = name
			body, _ := json.Marshal(d)
			s.docs[k] = body
			break
		}
	}
	return s.persist()
}
func (s *Store) Repair() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.ready(); e != nil {
		return e
	}
	s.rebuild()
	return s.persist()
}
func (s *Store) Delete(tenant, sku string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.ready(); e != nil {
		return e
	}
	found := false
	out := s.data.Products[:0]
	for _, p := range s.data.Products {
		if p.Tenant == tenant && p.SKU == sku {
			found = true
			continue
		}
		out = append(out, p)
	}
	if !found {
		return ErrInvalid
	}
	s.data.Products = out
	for i := range s.data.Customers {
		c := &s.data.Customers[i]
		if c.Tenant == tenant {
			v := c.Favorites[:0]
			for _, id := range c.Favorites {
				if id != sku {
					v = append(v, id)
				}
			}
			c.Favorites = v
		}
	}
	s.rebuild()
	return s.persist()
}
