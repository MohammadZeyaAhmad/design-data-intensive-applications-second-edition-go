package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/database"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
	"github.com/jackc/pgx/v5/pgxpool"
)

func inventoryReadRouter(ctx context.Context, primary *pgxpool.Pool) (*readrouting.Router, func(), error) {
	flag := os.Getenv("ATLAS_READ_ROUTING_ENABLED")
	if flag == "" || flag == "false" {
		return nil, func() {}, nil
	}
	if flag != "true" {
		return nil, nil, errors.New("ATLAS_READ_ROUTING_ENABLED must be true or false")
	}
	raw := os.Getenv("ATLAS_READ_REPLICAS")
	if raw == "" {
		raw = "{}"
	}
	var dsns map[string]string
	if len(raw) > 32768 || json.Unmarshal([]byte(raw), &dsns) != nil || len(dsns) > 8 {
		return nil, nil, errors.New("invalid ATLAS_READ_REPLICAS")
	}
	var replicas []readrouting.Replica
	closePools := func() {
		for _, r := range replicas {
			r.Pool.Close()
		}
	}
	labels := make([]string, 0, len(dsns))
	for name := range dsns {
		labels = append(labels, name)
	}
	sort.Strings(labels)
	for _, name := range labels {
		cfg, err := pgxpool.ParseConfig(dsns[name])
		if err != nil {
			closePools()
			return nil, nil, errors.New("invalid read replica configuration")
		}
		database.ConfigureHTTP(cfg)
		cfg.MaxConns = 4
		cfg.MinConns = 0
		cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			closePools()
			return nil, nil, errors.New("cannot create read replica pool")
		}
		replicas = append(replicas, readrouting.Replica{Label: name, Pool: pool})
	}
	startup, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	router, err := readrouting.New(startup, readrouting.Options{Primary: primary, Replicas: replicas, Key: []byte(os.Getenv("ATLAS_READ_TOKEN_KEY")), Shard: "inventory"})
	if err != nil {
		closePools()
		return nil, nil, err
	}
	return router, closePools, nil
}
