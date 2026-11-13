package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 000_platform/*.sql inventory/*.sql ordering/*.sql catalog/*.sql payments/*.sql checkout/*.sql fulfillment/*.sql messaging/*.sql search/*.sql
var files embed.FS

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7392018501)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.atlas_schema_migrations (name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(files, "*/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		checksum := hex.EncodeToString(sum[:])
		var existing string
		err = tx.QueryRow(ctx, "SELECT checksum FROM public.atlas_schema_migrations WHERE name=$1", name).Scan(&existing)
		if err == nil {
			if existing != checksum {
				return fmt.Errorf("migration checksum mismatch: %s", name)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// A source-controlled marker bounds lock acquisition for online expand
		// steps without changing the policy of historical migration files.
		var priorLockTimeout string
		bounded := strings.HasPrefix(string(data), "-- atlas:bounded-locks\n")
		if bounded {
			if err = tx.QueryRow(ctx, "SELECT current_setting('lock_timeout')").Scan(&priorLockTimeout); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "SELECT set_config('lock_timeout','250ms',true)"); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if bounded {
			if _, err = tx.Exec(ctx, "SELECT set_config('lock_timeout',$1,true)", priorLockTimeout); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, "INSERT INTO public.atlas_schema_migrations (name,checksum) VALUES ($1,$2)", name, checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func Check(ctx context.Context, pool *pgxpool.Pool) error {
	names, err := fs.Glob(files, "*/*.sql")
	if err != nil {
		return err
	}
	for _, name := range names {
		data, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		var checksum string
		if err = pool.QueryRow(ctx, "SELECT checksum FROM public.atlas_schema_migrations WHERE name=$1", name).Scan(&checksum); err != nil {
			return fmt.Errorf("migration required: %s", name)
		}
		if checksum != hex.EncodeToString(sum[:]) {
			return fmt.Errorf("migration checksum mismatch: %s", name)
		}
	}
	return nil
}
