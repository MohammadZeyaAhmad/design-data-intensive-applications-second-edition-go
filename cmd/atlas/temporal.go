package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	checktemporal "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/temporal"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

func temporalCommand(ctx context.Context, logger *slog.Logger, store *checkdb.Store, activities checktemporal.Activities, command string) error {
	address := os.Getenv("ATLAS_TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	namespace := os.Getenv("ATLAS_TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	c, err := client.DialContext(startup, client.Options{HostPort: address, Namespace: namespace, Logger: temporallog.NewStructuredLogger(logger)})
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	if command == "temporal-check" {
		check, stop := context.WithTimeout(ctx, 3*time.Second)
		defer stop()
		_, err = c.CheckHealth(check, &client.CheckHealthRequest{})
		return err
	}
	if command == "temporal-worker" {
		w := worker.New(c, checktemporal.TaskQueue, worker.Options{WorkerStopTimeout: 5 * time.Second})
		checktemporal.Register(w, activities)
		if err = w.Start(); err != nil {
			return err
		}
		logger.Info("Temporal checkout worker started", "namespace", namespace)
		<-ctx.Done()
		w.Stop()
		return ctx.Err()
	}
	starter := checktemporal.Starter{Client: c}
	logger.Info("Temporal checkout start and control dispatcher started")
	for ctx.Err() == nil {
		step, stop := context.WithTimeout(ctx, 5*time.Second)
		started, startErr := store.StartTemporalOne(step, starter)
		woken, wakeErr := store.WakeTemporalOne(step, starter)
		stop()
		if startErr != nil || wakeErr != nil {
			logger.Error("Temporal dispatch retry")
		}
		if !started && !woken || startErr != nil || wakeErr != nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	return ctx.Err()
}
