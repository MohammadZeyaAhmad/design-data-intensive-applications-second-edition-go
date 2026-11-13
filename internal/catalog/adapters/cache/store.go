package cache

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/resilience"
)

type Source interface {
	ports.Store
	ports.Content
}
type Options struct {
	PositiveTTL, NegativeTTL, LoadTimeout, FloorTTL time.Duration
	JitterPercent                                   int
	MaxFlights                                      int
	RefreshAhead                                    time.Duration
	WriteThrough                                    bool
}

func Defaults() Options {
	return Options{PositiveTTL: 750 * time.Millisecond, NegativeTTL: 100 * time.Millisecond, LoadTimeout: time.Second, FloorTTL: 30 * time.Second, JitterPercent: 20, MaxFlights: 64}
}

type loaded struct {
	product  domain.Product
	missing  bool
	fallback bool
}
type Store struct {
	Source
	backend                                                                                                       Backend
	options                                                                                                       Options
	group                                                                                                         *resilience.Group[loaded]
	tenants                                                                                                       *resilience.Tenants
	Origin, Cache                                                                                                 *resilience.Policy
	hits, misses, negative, fallback, corrupt, loads, invalidations, invalidationsFailed, putsRejected, refreshes atomic.Int64
}
type Stats struct {
	Refreshes           int64                  `json:"refreshes"`
	Tenants             resilience.TenantStats `json:"tenant_admission"`
	Hits                int64                  `json:"hits"`
	Misses              int64                  `json:"misses"`
	Negative            int64                  `json:"negative_hits"`
	Fallback            int64                  `json:"fallbacks"`
	Corrupt             int64                  `json:"corrupt"`
	Loads               int64                  `json:"origin_loads"`
	Invalidations       int64                  `json:"invalidations"`
	InvalidationsFailed int64                  `json:"invalidations_failed"`
	PutsRejected        int64                  `json:"puts_rejected"`
	Flights             resilience.FlightStats `json:"flights"`
	Origin              resilience.PolicyStats `json:"origin"`
	Cache               resilience.PolicyStats `json:"cache"`
}

func New(source Source, backend Backend, o Options) (*Store, error) {
	if source == nil || backend == nil || o.PositiveTTL < 10*time.Millisecond || o.PositiveTTL > 10*time.Second || o.NegativeTTL < time.Millisecond || o.NegativeTTL > o.PositiveTTL || o.LoadTimeout < 10*time.Millisecond || o.LoadTimeout > 5*time.Second || o.FloorTTL < 2*(o.PositiveTTL+o.LoadTimeout) || o.FloorTTL > time.Minute || o.JitterPercent < 0 || o.JitterPercent > 50 || o.RefreshAhead < 0 || o.RefreshAhead > o.PositiveTTL/2 {
		return nil, errors.New("invalid catalog cache options")
	}
	group, e := resilience.NewGroup[loaded](o.MaxFlights, o.LoadTimeout)
	if e != nil {
		return nil, e
	}
	origin, e := resilience.NewPolicy(8, 16, 2, o.LoadTimeout, o.LoadTimeout/2)
	if e != nil {
		return nil, e
	}
	cache, e := resilience.NewPolicy(8, 16, 1, 100*time.Millisecond, 80*time.Millisecond)
	if e != nil {
		return nil, e
	}
	origin.Classify = func(e error) resilience.Classification {
		if e == nil || errors.Is(e, api.ErrNotFound) || errors.Is(e, api.ErrInvalid) || errors.Is(e, domain.ErrInvalid) {
			return resilience.Classification{Outcome: resilience.Healthy}
		}
		return resilience.Classification{Retry: true, Outcome: resilience.Failed}
	}
	tenants, _ := resilience.NewTenants(2, 4, 64)
	return &Store{tenants: tenants, Source: source, backend: backend, options: o, group: group, Origin: origin, Cache: cache}, nil
}
func (s *Store) ttl(base time.Duration) time.Duration {
	spread := int64(base) * int64(s.options.JitterPercent) / 100
	if spread == 0 {
		return base
	}
	return base - time.Duration(rand.Int64N(spread+1))
}
func (s *Store) Product(ctx context.Context, tenant, sku string) (domain.Product, error) {
	release, err := s.tenants.Acquire(ctx, tenant)
	if err != nil {
		return domain.Product{}, err
	}
	defer release()
	return resilience.Do(ctx, s.Origin, resilience.ReadOnly, func(ctx context.Context) (domain.Product, error) { return s.Source.Product(ctx, tenant, sku) })
}
func (s *Store) CachedProduct(ctx context.Context, tenant, sku string, minVersion int64) (domain.Product, string, error) {
	if !tenantPattern.MatchString(tenant) || !skuPattern.MatchString(sku) || minVersion < 0 {
		return domain.Product{}, "", api.ErrInvalid
	}
	name := key(tenant, sku)
	for attempt := 0; attempt < 2; attempt++ {
		snapshot, cacheErr := resilience.Do(ctx, s.Cache, resilience.ReadOnly, func(ctx context.Context) (Snapshot, error) { return s.backend.Get(ctx, name) })
		if cacheErr == nil && len(snapshot.Payload) > 0 {
			value, e := decode(tenant, sku, snapshot)
			if e == nil {
				if value.Missing && minVersion == 0 {
					s.negative.Add(1)
					return domain.Product{}, "negative", api.ErrNotFound
				}
				if !value.Missing && value.Product.Version >= minVersion {
					s.hits.Add(1)
					if s.options.RefreshAhead > 0 && snapshot.Remaining <= s.options.RefreshAhead {
						if s.group.Start(ctx, name, func(c context.Context) (loaded, error) { return s.load(c, tenant, sku, false) }) {
							s.refreshes.Add(1)
						}
					}
					return value.Product, "hit", nil
				}
			} else {
				s.corrupt.Add(1)
				_, _ = resilience.Do(ctx, s.Cache, resilience.Idempotent, func(ctx context.Context) (bool, error) { return true, s.backend.Discard(ctx, name) })
			}
		}
		s.misses.Add(1)
		if cacheErr != nil {
			s.fallback.Add(1)
		}
		value, shared, e := s.group.Do(ctx, name, func(loadCtx context.Context) (loaded, error) {
			// Recheck after winning admission: another load may have filled the cache
			// between a caller's initial miss and the start of this bounded flight.
			peek, pe := resilience.Do(loadCtx, s.Cache, resilience.ReadOnly, func(ctx context.Context) (Snapshot, error) { return s.backend.Get(ctx, name) })
			if pe == nil && len(peek.Payload) > 0 {
				cached, ce := decode(tenant, sku, peek)
				if ce == nil && ((cached.Missing && minVersion == 0) || (!cached.Missing && cached.Product.Version >= minVersion)) {
					return loaded{product: cached.Product, missing: cached.Missing}, nil
				}
			}
			return s.load(loadCtx, tenant, sku, cacheErr != nil || pe != nil)
		})
		if errors.Is(e, resilience.ErrInvalidated) && attempt == 0 {
			continue
		}
		if e != nil {
			return domain.Product{}, "miss", e
		}
		status := "miss"
		if shared {
			status = "coalesced"
		}
		if value.fallback {
			status = "fallback"
		}
		if value.missing {
			if minVersion > 0 && attempt == 0 {
				continue
			}
			return domain.Product{}, status, api.ErrNotFound
		}
		if value.product.Version < minVersion {
			if attempt == 0 {
				continue
			}
			return domain.Product{}, status, api.ErrConflict
		}
		return value.product, status, nil
	}
	return domain.Product{}, "miss", api.ErrConflict
}
func (s *Store) load(loadCtx context.Context, tenant, sku string, fallback bool) (loaded, error) {
	s.loads.Add(1)
	product, originErr := s.Product(loadCtx, tenant, sku)
	missing := errors.Is(originErr, api.ErrNotFound)
	if originErr != nil && !missing {
		return loaded{}, originErr
	}
	if e := loadCtx.Err(); e != nil {
		return loaded{}, e
	}
	result := loaded{product: product, missing: missing, fallback: fallback}
	body, encodeErr := encode(tenant, sku, product, missing)
	if encodeErr != nil {
		return loaded{}, encodeErr
	}
	ttl := s.options.PositiveTTL
	version := product.Version
	if missing {
		ttl = s.options.NegativeTTL
		version = 0
	}
	accepted, putErr := resilience.Do(loadCtx, s.Cache, resilience.Idempotent, func(ctx context.Context) (bool, error) {
		return s.backend.Put(ctx, key(tenant, sku), version, body, s.ttl(ttl), s.options.FloorTTL)
	})
	if putErr != nil {
		result.fallback = true
	} else if !accepted {
		s.putsRejected.Add(1)
	}
	return result, nil
}
func (s *Store) Publish(ctx context.Context, r commands.Request, p domain.Product) (domain.Product, bool, error) {
	out, replayed, e := s.Source.Publish(ctx, r, p)
	if e == nil && !replayed {
		s.Invalidate(ctx, r.Tenant, out.SKU, out.Version)
		if s.options.WriteThrough {
			if b, err := encode(r.Tenant, out.SKU, out, false); err == nil {
				_, _ = resilience.Do(ctx, s.Cache, resilience.Idempotent, func(c context.Context) (bool, error) {
					return s.backend.Put(c, key(r.Tenant, out.SKU), out.Version, b, s.ttl(s.options.PositiveTTL), s.options.FloorTTL)
				})
			}
		}
	}
	return out, replayed, e
}
func (s *Store) Invalidate(parent context.Context, tenant, sku string, version int64) {
	if !tenantPattern.MatchString(tenant) || !skuPattern.MatchString(sku) || version < 1 {
		return
	}
	name := key(tenant, sku)
	s.group.Invalidate(name)
	s.invalidations.Add(1)
	// The business transaction has committed already. This bounded cache action
	// must not turn its success into a failed write or extend the caller budget.
	ctx, cancel := context.WithTimeout(parent, 100*time.Millisecond)
	defer cancel()
	_, e := resilience.Do(ctx, s.Cache, resilience.Idempotent, func(ctx context.Context) (bool, error) {
		return true, s.backend.Invalidate(ctx, name, version, s.options.FloorTTL)
	})
	if e != nil {
		s.invalidationsFailed.Add(1)
	}
}
func (s *Store) Close(ctx context.Context) error {
	s.Origin.Bulkhead.BeginDrain()
	s.Cache.Bulkhead.BeginDrain()
	if err := s.group.Close(ctx); err != nil {
		return err
	}
	if err := s.Origin.Bulkhead.Drain(ctx); err != nil {
		return err
	}
	return s.Cache.Bulkhead.Drain(ctx)
}
func (s *Store) Stats() Stats {
	return Stats{Refreshes: s.refreshes.Load(), Tenants: s.tenants.Stats(), Hits: s.hits.Load(), Misses: s.misses.Load(), Negative: s.negative.Load(), Fallback: s.fallback.Load(), Corrupt: s.corrupt.Load(), Loads: s.loads.Load(), Invalidations: s.invalidations.Load(), InvalidationsFailed: s.invalidationsFailed.Load(), PutsRejected: s.putsRejected.Load(), Flights: s.group.Stats(), Origin: s.Origin.Stats(), Cache: s.Cache.Stats()}
}
