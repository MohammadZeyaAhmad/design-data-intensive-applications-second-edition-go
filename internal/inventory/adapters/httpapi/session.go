package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
)

var sessionHeaders = []string{"X-Session-Token", "X-Read-Consistency", "X-Read-On-Lag", "X-Read-Wait-Ms"}

func options(r *http.Request) (ports.ReadOptions, error) {
	o := ports.ReadOptions{Token: r.Header.Get("X-Session-Token"), Mode: "primary", OnLag: "pending", Wait: 100 * time.Millisecond}
	for _, h := range sessionHeaders {
		if len(r.Header.Values(h)) > 1 || len(r.Header.Get(h)) > readrouting.MaxTokenBytes {
			return o, readrouting.ErrContract
		}
	}
	if s := r.Header.Get("X-Read-Consistency"); s != "" {
		o.Mode = s
	}
	if s := r.Header.Get("X-Read-On-Lag"); s != "" {
		o.OnLag = s
	}
	if s := r.Header.Get("X-Read-Wait-Ms"); s != "" {
		n, e := strconv.Atoi(s)
		if e != nil || n < 0 || n > 2000 {
			return o, readrouting.ErrContract
		}
		o.Wait = time.Duration(n) * time.Millisecond
	}
	if o.Mode != "primary" && o.Mode != "replica" || o.OnLag != "pending" && o.OnLag != "primary" {
		return o, readrouting.ErrContract
	}
	return o, nil
}
func hasSessionHeaders(r *http.Request) bool {
	for _, h := range sessionHeaders {
		if len(r.Header.Values(h)) > 0 {
			return true
		}
	}
	return false
}
func sessionMeta(w http.ResponseWriter, m ports.ReadMeta) {
	w.Header().Set("Cache-Control", "no-store")
	if m.Token != "" {
		w.Header().Set("X-Session-Token", m.Token)
		w.Header().Set("X-Session-Status", "issued")
	}
	if m.Source != "" {
		w.Header().Set("X-Read-Source", m.Source)
	}
	if m.Reason != "" {
		w.Header().Set("X-Read-Reason", m.Reason)
	}
}
func (s server) finish(w http.ResponseWriter, r *http.Request, status int, out ports.Outcome, replayed bool, err error) {
	if err == nil && s.Session != nil {
		m, e := s.Session.AfterWrite(r.Context(), tenant(r), r.Header.Get("X-Session-Token"))
		if e != nil {
			// The business transaction has already committed. Preserve its
			// receipt; a client must use primary reads until a fence is obtained.
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Session-Status", "unavailable")
			s.Logger.Warn("committed inventory command has no session fence")
		} else {
			sessionMeta(w, m)
		}
	}
	s.result(w, status, out, replayed, err)
}
func (s server) query(w http.ResponseWriter, r *http.Request, q ports.ViewQuery) (ports.View, bool) {
	o, e := options(r)
	if e != nil {
		s.failure(w, e)
		return ports.View{}, false
	}
	v, m, e := s.Session.QueryView(r.Context(), tenant(r), q, o)
	if e == nil || errors.Is(e, domain.ErrNotFound) {
		sessionMeta(w, m)
	}
	if e != nil {
		s.failure(w, e)
		return ports.View{}, false
	}
	return v, true
}
func (s server) snapshot(w http.ResponseWriter, r *http.Request) {
	if s.Session == nil {
		s.failure(w, readrouting.ErrContract)
		return
	}
	q := ports.ViewQuery{SKU: r.URL.Query().Get("sku"), WarehouseID: r.URL.Query().Get("warehouse_id"), ReservationID: r.URL.Query().Get("reservation_id")}
	if !domain.ValidSKU(q.SKU) || !domain.ValidWarehouse(q.WarehouseID) || !identity.ValidID(q.ReservationID) {
		s.failure(w, domain.ErrInvalidInput)
		return
	}
	v, ok := s.query(w, r, q)
	if ok {
		write(w, 200, v)
	}
}
