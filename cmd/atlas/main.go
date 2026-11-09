package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/httpapi"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	ordershttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/httpapi"
	orderspostgres "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("atlas stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	if len(os.Args) != 2 {
		return errors.New("usage: atlas serve|worker|migrate|check")
	}
	command := os.Args[1]
	if command != "serve" && command != "worker" && command != "migrate" && command != "check" {
		return errors.New("unknown atlas command")
	}
	if err := config.LoadLocalEnv(".env"); err != nil {
		return err
	}
	dsn := os.Getenv("ATLAS_DATABASE_URL")
	if dsn == "" {
		return errors.New("ATLAS_DATABASE_URL is required")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	poolConfig.MaxConns = 16
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return errors.New("could not initialize database pool")
	}
	defer pool.Close()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = pool.Ping(startup); err != nil {
		return errors.New("database is unavailable")
	}
	if command == "migrate" {
		if err = migrations.Apply(startup, pool); err != nil {
			return err
		}
		logger.Info("migrations applied")
		return nil
	}
	if err = migrations.Check(startup, pool); err != nil {
		return err
	}
	if command == "check" {
		logger.Info("database and schema ready")
		return nil
	}
	store := &postgres.Store{Pool: pool}
	orderStore := &orderspostgres.Store{Pool: pool}
	if command == "worker" {
		logger.Info("Atlas inventory and ordering workers started")
		group, workerContext := errgroup.WithContext(ctx)
		group.Go(func() error {
			return store.Run(workerContext, func(err error) { logger.Error("inventory worker attempt failed", "error", err) })
		})
		group.Go(func() error {
			return orderStore.Run(workerContext, func(err error) { logger.Error("ordering projection attempt failed", "error", err) })
		})
		return group.Wait()
	}
	keys, err := config.TenantKeys(os.Getenv("ATLAS_TENANT_KEYS"))
	if err != nil {
		return err
	}
	address := os.Getenv("ATLAS_HTTP_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	mux := http.NewServeMux()
	orderHTTP := ordershttp.New(ordershttp.Options{Store: orderStore, TenantKeys: keys, Logger: logger})
	mux.Handle("/v1/orders", orderHTTP)
	mux.Handle("/v1/orders/", orderHTTP)
	mux.Handle("/", httpapi.New(httpapi.Options{Store: store, TenantKeys: keys, Ready: func(ctx context.Context) error { return migrations.Check(ctx, pool) }, Logger: logger}))
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	logger.Info("Atlas API started", "address", address, "authentication", "development tenant keys")
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err = server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
