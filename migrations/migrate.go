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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed inventory/*.sql ordering/*.sql catalog/*.sql payments/*.sql checkout/*.sql
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
		if _, err = tx.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
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
