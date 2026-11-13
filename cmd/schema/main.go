// schema operates the Catalog expand/backfill/validate/contract rollout. It is
// a local operator command, separate from public application routes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	catalogdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/atomicfile"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, "schema:", e)
		os.Exit(1)
	}
}
func run(parent context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: schema status|prepare|batch|run|validate|window|lease|retire-reader|retire-rollback|contract|export|replay [flags]")
	}
	command := args[0]
	flags := flag.NewFlagSet("schema "+command, flag.ContinueOnError)
	limit := flags.Int("limit", 32, "rows per bounded batch (1..128)")
	interval := flags.Duration("interval", 100*time.Millisecond, "durable minimum batch interval (10ms..1m)")
	duration := flags.Duration("window", 7*24*time.Hour, "rollback retention window (1s..30d)")
	ttl := flags.Duration("ttl", time.Minute, "reader inventory lease (100ms..1h)")
	id := flags.String("id", "", "batch or reader identity")
	capability := flags.Int("capability", 1, "reader database format capability: 1 legacy, 2 canonical")
	operator := flags.String("operator", "operator:local", "operator audit identity")
	reason := flags.String("reason", "", "required reason for rollout decisions")
	tenant := flags.String("tenant", "", "owned tenant for history export")
	brand := flags.String("brand", "", "brand identity for history export")
	file := flags.String("file", "", "immutable archive output or replay input")
	maxBatches := flags.Int("max-batches", 10000, "maximum batches per resumable run (1..10000)")
	deadline := flags.Duration("deadline", 5*time.Minute, "operator command deadline (1s..1h)")
	if e := flags.Parse(args[1:]); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected schema argument")
	}
	if *deadline < time.Second || *deadline > time.Hour {
		return errors.New("deadline must be 1s..1h")
	}
	ctx, cancel := context.WithTimeout(parent, *deadline)
	defer cancel()
	var result any
	if command == "replay" {
		f, e := os.Open(*file)
		if e != nil {
			return e
		}
		defer f.Close()
		raw, e := io.ReadAll(io.LimitReader(f, api.MaxBrandArchive+1))
		if e != nil {
			return e
		}
		records, e := api.ReplayBrandArchive(raw)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(records)
	}
	dsn := os.Getenv("ATLAS_SCHEMA_DATABASE_URL")
	if dsn == "" {
		return errors.New("ATLAS_SCHEMA_DATABASE_URL is required")
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		return errors.New("invalid schema database configuration")
	}
	cfg.MaxConns = 4
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return errors.New("could not initialize schema database")
	}
	defer pool.Close()
	if pool.Ping(ctx) != nil {
		return errors.New("schema database is unavailable")
	}
	store := &catalogdb.Store{Pool: pool}
	switch command {
	case "status":
		result, e = store.BrandRollout(ctx)
	case "prepare":
		e = store.PrepareBrandBackfill(ctx)
	case "batch":
		result, e = store.BackfillBrandNames(ctx, *id, *limit, *interval)
	case "run":
		if len(*id) < 1 || len(*id) > 48 || *maxBatches < 1 || *maxBatches > 10000 {
			return errors.New("run requires an id up to 48 characters and 1..10000 batches")
		}
		if e = store.PrepareBrandBackfill(ctx); e != nil {
			return e
		}
		batches := []catalogdb.BrandBatch{}
		complete := false
		for i := 0; i < *maxBatches; {
			batch, err := store.BackfillBrandNames(ctx, fmt.Sprintf("%s-%06d", *id, i), *limit, *interval)
			if err != nil {
				return err
			}
			if batch.Throttled {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(*interval):
				}
				continue
			}
			batches = append(batches, batch)
			i++
			if !batch.Pending {
				complete = true
				break
			}
		}
		result = map[string]any{"complete": complete, "batches": batches}
	case "validate":
		e = store.ValidateBrandNames(ctx, *operator, *reason)
	case "window":
		e = store.OpenBrandRollbackWindow(ctx, *duration, *operator, *reason)
	case "lease":
		e = store.LeaseBrandReader(ctx, *id, *capability, *ttl)
	case "retire-reader":
		e = store.RetireBrandReader(ctx, *id, *operator, *reason)
	case "retire-rollback":
		e = store.RetireBrandRollback(ctx, *operator, *reason)
	case "contract":
		e = store.ContractBrandNames(ctx, *operator, *reason)
	case "export":
		var records []api.BrandSourceRecord
		records, e = store.BrandHistory(ctx, *tenant, *brand)
		if e == nil {
			var raw []byte
			raw, e = api.EncodeBrandArchive(records)
			if e == nil {
				e = atomicfile.WriteOnce(*file, raw)
				result = map[string]any{"records": len(records), "archive_sha256": api.HistoryDigest(raw)}
			}
		}
	default:
		return errors.New("unknown schema command")
	}
	if e != nil {
		return e
	}
	if result == nil {
		result = map[string]bool{"ok": true}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
