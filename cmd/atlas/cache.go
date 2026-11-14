package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	catcache "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/cache"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/resilience"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry"
)

func catalogCache(ctx context.Context, logger *slog.Logger, dsn string, source catcache.Source) (*catcache.Store, func(), error) {
	var backend catcache.Backend
	var closeBackend func() error
	switch os.Getenv("ATLAS_CACHE_BACKEND") {
	case "", "memory":
		backend, _ = catcache.NewMemory(1024)
	case "redis":
		r, err := catcache.NewRedis(os.Getenv("ATLAS_CACHE_ADDRESS"), os.Getenv("ATLAS_CACHE_PASSWORD"), "atlas:catalog:v1")
		if err != nil {
			return nil, nil, err
		}
		backend, closeBackend = r, r.Close
	default:
		return nil, nil, errors.New("ATLAS_CACHE_BACKEND must be memory or redis")
	}
	o := catcache.Defaults()
	var refresh bool
	for name, target := range map[string]*bool{"ATLAS_CACHE_REFRESH_AHEAD": &refresh, "ATLAS_CACHE_WRITE_THROUGH": &o.WriteThrough} {
		value := os.Getenv(name)
		if value != "" {
			v, err := strconv.ParseBool(value)
			if err != nil {
				if closeBackend != nil {
					_ = closeBackend()
				}
				return nil, nil, errors.New("invalid cache mode")
			}
			*target = v
		}
	}
	if refresh {
		o.RefreshAhead = 150 * time.Millisecond
	}
	store, err := catcache.New(source, backend, o)
	if err != nil {
		if closeBackend != nil {
			_ = closeBackend()
		}
		return nil, nil, err
	}
	if observer := telemetry.From(ctx); observer != nil {
		observer.RegisterBulkhead("catalog_origin", store.Origin.Bulkhead.Stats)
		observer.RegisterBulkhead("catalog_cache", store.Cache.Bulkhead.Stats)
	}
	listener := catcache.StartListener(ctx, dsn, store)
	cleanup := func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = listener.Close(shutdown)
		if err := store.Close(shutdown); err != nil {
			logger.Error("catalog cache drain failed")
		}
		if closeBackend != nil {
			_ = closeBackend()
		}
		stats := store.Stats()
		logger.Info("catalog cache stopped", "cache_hits", stats.Hits, "cache_misses", stats.Misses, "cache_fallbacks", stats.Fallback, "cache_loads", stats.Loads, "cache_invalidations_failed", stats.InvalidationsFailed)
	}
	return store, cleanup, nil
}

func httpAdmission() (*resilience.HTTPAdmission, error) {
	o := resilience.DefaultHTTPOptions()
	for name, target := range map[string]*int{"ATLAS_HTTP_READ_ACTIVE": &o.ReadActive, "ATLAS_HTTP_READ_QUEUED": &o.ReadQueued, "ATLAS_HTTP_WRITE_ACTIVE": &o.WriteActive, "ATLAS_HTTP_WRITE_QUEUED": &o.WriteQueued, "ATLAS_HTTP_RATE": &o.Rate, "ATLAS_HTTP_BURST": &o.Burst} {
		if value := os.Getenv(name); value != "" {
			v, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.New("invalid HTTP admission configuration")
			}
			*target = v
		}
	}
	return resilience.NewHTTPAdmission(o)
}
