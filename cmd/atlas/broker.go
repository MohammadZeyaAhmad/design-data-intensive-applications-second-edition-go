package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	fulfill "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	fulfilldb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/adapters/postgres"
	invdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	orderdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/kafka"
	inboxdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func brokers() ([]string, error) {
	raw := os.Getenv("ATLAS_KAFKA_BROKERS")
	if raw == "" {
		return nil, errors.New("ATLAS_KAFKA_BROKERS is required")
	}
	return api.Brokers(raw)
}
func brokerCommand(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, command string) error {
	inbox := inboxdb.Inbox{Pool: pool, Group: api.Group}
	if command == "broker-status" {
		stats, err := inbox.Stats(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(stats)
	}
	addresses, err := brokers()
	if err != nil {
		return err
	}
	if command == "broker-init" {
		step, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return kafka.Check(step, addresses, true)
	}
	if command == "broker-consumer" {
		return (kafka.Consumer{Brokers: addresses, Group: api.Group, Stager: inbox}).Run(ctx, func(error) { logger.Error("broker consumption retry") })
	}
	step, cancel := context.WithTimeout(ctx, 10*time.Second)
	p, err := kafka.NewPublisher(step, addresses)
	cancel()
	if err != nil {
		return err
	}
	defer p.Close()
	i, o := &invdb.Store{Pool: pool}, &orderdb.Store{Pool: pool}
	lanes := []func(context.Context, api.Publisher) (bool, error){i.PublishOne, o.PublishOne, o.PublishCheckout, o.PublishFulfillment}
	for ctx.Err() == nil {
		worked := false
		failed := false
		for _, publish := range lanes {
			step, cancel := context.WithTimeout(ctx, 3*time.Second)
			did, err := publish(step, p)
			cancel()
			worked = worked || did
			if err != nil && ctx.Err() == nil {
				failed = true
				logger.Error("broker publication retry")
			}
		}
		if !worked || failed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
		}
	}
	return ctx.Err()
}

func brokerHandler(i *invdb.Store, o *orderdb.Store, c *checkdb.Store, f *fulfilldb.Store) api.Handler {
	return api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		switch topic {
		case api.InventoryTopic:
			return i.ApplyMessage(ctx, m)
		case api.OrderingTopic:
			return o.ApplyMessage(ctx, m)
		case api.CheckoutTopic:
			if err := o.ValidateRequest(ctx, topic, m); err != nil {
				return err
			}
			var r orders.Requested
			if err := json.Unmarshal(m.Data, &r); err != nil {
				return err
			}
			if r.ID != m.ID || r.TenantID != m.TenantID || r.OrderID != m.StreamID || r.Metadata != m.Metadata {
				return api.ErrSource
			}
			return c.Submit(ctx, r)
		case api.FulfillmentTopic:
			if err := o.ValidateRequest(ctx, topic, m); err != nil {
				return err
			}
			var r fulfill.Request
			if err := json.Unmarshal(m.Data, &r); err != nil {
				return err
			}
			if r.ID != m.ID || r.TenantID != m.TenantID || r.OrderID != m.StreamID || r.Metadata != m.Metadata {
				return api.ErrSource
			}
			return f.Submit(ctx, r)
		}
		return api.ErrInvalid
	})
}
