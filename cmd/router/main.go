package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/routing"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx); e != nil {
		slog.Error("router stopped", "error", e)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	path := os.Getenv("ATLAS_RPC_REGISTRY")
	if path == "" {
		return errors.New("ATLAS_RPC_REGISTRY is required")
	}
	r, e := routing.ReadRegistry(path)
	if e != nil {
		return e
	}
	pool, e := routing.NewLocal(r)
	if e != nil {
		return e
	}
	defer pool.Close()
	address := os.Getenv("ATLAS_ROUTER_ADDR")
	if address == "" {
		address = "127.0.0.1:18084"
	}
	listener, e := net.Listen("tcp", address)
	if e != nil {
		return e
	}
	defer listener.Close()
	g, h := routing.NewProxy(pool)
	healthCtx, stop := context.WithCancel(ctx)
	defer stop()
	go pool.Watch(healthCtx, path, func(error) { slog.Warn("registry update rejected; retaining last valid revision") })
	go pool.Monitor(healthCtx, h)
	done := make(chan error, 1)
	go func() { done <- g.Serve(listener) }()
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
