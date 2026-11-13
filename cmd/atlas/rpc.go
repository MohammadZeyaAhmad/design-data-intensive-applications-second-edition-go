package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/grpcapi"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func serveRPC(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool) error {
	keys, e := config.TenantKeys(os.Getenv("ATLAS_TENANT_KEYS"))
	if e != nil {
		return e
	}
	address := os.Getenv("ATLAS_GRPC_ADDR")
	if address == "" {
		address = "127.0.0.1:18083"
	}
	endpoint := os.Getenv("ATLAS_RPC_ENDPOINT")
	if endpoint == "" {
		endpoint = "local"
	}
	if len(endpoint) > 48 {
		return errors.New("RPC endpoint label too long")
	}
	listener, e := net.Listen("tcp", address)
	if e != nil {
		return errors.New("could not bind RPC listener")
	}
	defer listener.Close()
	ready := func(ctx context.Context) error { return migrations.Check(ctx, pool) }
	g, h := grpcapi.New(grpcapi.Options{Store: &postgres.Store{Pool: pool}, TenantKeys: keys, Endpoint: endpoint, Logger: logger})
	healthCtx, stop := context.WithCancel(ctx)
	defer stop()
	go grpcapi.MonitorHealth(healthCtx, h, ready)
	done := make(chan error, 1)
	go func() { done <- g.Serve(listener) }()
	logger.Info("Atlas Inventory RPC started", "address", address, "authentication", "development tenant keys")
	select {
	case e := <-done:
		return e
	case <-ctx.Done():
		h.Shutdown()
		drained := make(chan struct{})
		go func() { g.GracefulStop(); close(drained) }()
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			g.Stop()
			<-drained
		}
		return nil
	}
}
