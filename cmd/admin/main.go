package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	checkoutdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	fulfilldb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/operator"
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
		return errors.New("usage: admin snapshot-order|rebuild-order|workflow-status|workflow-control|workflow-history -tenant TENANT -id UUID")
	}
	mode := os.Args[1]
	if mode != "snapshot-order" && mode != "rebuild-order" && mode != "workflow-status" && mode != "workflow-control" && mode != "workflow-history" {
		return errors.New("unknown admin operation")
	}
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	tenant := flags.String("tenant", "", "tenant owning this stream")
	id := flags.String("id", "", "resource UUID")
	kind := flags.String("kind", "", "checkout or shipment")
	action := flags.String("action", "", "pause, resume, or retry")
	reason := flags.String("reason", "", "investigation, dependency_restored, or operator_retry")
	key := flags.String("key", "", "stable command idempotency key")
	expected := flags.Int64("expected-control-version", -1, "required current control version")
	after := flags.Int64("after-version", 0, "exclusive control history version")
	limit := flags.Int("limit", 50, "history page size, at most 100")
	correlation := flags.String("correlation-id", "", "optional diagnostic correlation UUID")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *tenant == "" || *id == "" {
		return errors.New("exact tenant and order ID are required")
	}
	if strings.HasPrefix(mode, "workflow-") && (*kind != "checkout" && *kind != "shipment") {
		return errors.New("workflow kind must be checkout or shipment")
	}
	if mode == "workflow-control" && (*expected < 0 || *key == "" || *action == "" || *reason == "") {
		return errors.New("workflow control requires action, reason, key, and expected-control-version")
	}
	if *correlation != "" && !envelope.ValidID(*correlation) {
		return errors.New("invalid correlation UUID")
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
	if strings.HasPrefix(mode, "workflow-") {
		ctx, err = operator.Local(ctx, *tenant)
		if err != nil {
			return err
		}
		requestID, err := identity.NewID()
		if err != nil {
			return err
		}
		corr := *correlation
		if corr == "" {
			corr = requestID
		}
		ctx = envelope.With(ctx, envelope.Metadata{Version: 1, Actor: "operator:local", CorrelationID: corr, CausationID: requestID, OriginRequestID: requestID})
		var target api.Workflow
		if *kind == "checkout" {
			target = &checkoutdb.Store{Pool: pool}
		} else {
			target = &fulfilldb.Store{Pool: pool}
		}
		var output any
		switch mode {
		case "workflow-status":
			output, err = target.Inspect(ctx, *tenant, *id)
		case "workflow-history":
			output, err = target.ControlHistory(ctx, *tenant, *id, *after, *limit)
		case "workflow-control":
			var result api.Result
			var replayed bool
			result, replayed, err = target.Control(ctx, api.Command{TenantID: *tenant, ID: *id, Action: *action, Reason: *reason, Key: *key, ExpectedVersion: *expected})
			output = struct {
				Result   api.Result `json:"result"`
				Replayed bool       `json:"replayed"`
			}{result, replayed}
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(output)
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
