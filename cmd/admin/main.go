package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "admin:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: admin snapshot-order|rebuild-order -tenant TENANT -id ORDER_UUID")
	}
	mode := os.Args[1]
	if mode != "snapshot-order" && mode != "rebuild-order" {
		return errors.New("unknown admin operation")
	}
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	tenant := flags.String("tenant", "", "tenant owning this stream")
	id := flags.String("id", "", "order UUID")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *tenant == "" || *id == "" {
		return errors.New("exact tenant and order ID are required")
	}
	if err := config.LoadLocalEnv(".env"); err != nil {
		return err
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("ATLAS_DATABASE_URL"))
	if err != nil || os.Getenv("ATLAS_DATABASE_URL") == "" {
		return errors.New("invalid database configuration")
	}
	cfg.MaxConns = 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return errors.New("database unavailable")
	}
	defer pool.Close()
	if err = migrations.Check(ctx, pool); err != nil {
		return err
	}
	store := &postgres.Store{Pool: pool}
	var version int64
	if mode == "snapshot-order" {
		state, err := store.Snapshot(ctx, *tenant, *id)
		if err != nil {
			return err
		}
		version = state.Version
	} else {
		state, err := store.Rebuild(ctx, *tenant, *id)
		if err != nil {
			return err
		}
		version = state.Version
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"operation": mode, "order_id": *id, "version": version, "source": "full_event_replay"})
}
