package cmdtx

import (
	"bytes"
	"context"

	"encoding/json"
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"time"
)

// Execute supplies only request deduplication and transaction lifetime. Schema
// ownership and every business operation remain in the calling context.
func Execute[T any](ctx context.Context, pool *pgxpool.Pool, schema string, r commands.Request, fn func(pgx.Tx) (T, error)) (T, bool, error) {
	var zero T
	tx, err := pool.Begin(ctx)
	if err != nil {
		return zero, false, err
	}
	defer Rollback(tx)
	table := pgx.Identifier{schema, "command_results"}.Sanitize()
	var key string
	err = tx.QueryRow(ctx, "INSERT INTO "+table+" (tenant_id,operation,idempotency_key,request_hash) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING idempotency_key", r.Tenant, r.Operation, r.Key, r.Hash[:]).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		var hash, body []byte
		if err = tx.QueryRow(ctx, "SELECT request_hash,response FROM "+table+" WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3", r.Tenant, r.Operation, r.Key).Scan(&hash, &body); err != nil {
			return zero, false, err
		}
		if !bytes.Equal(hash, r.Hash[:]) {
			return zero, false, commands.ErrConflict
		}
		var out T
		if err = json.Unmarshal(body, &out); err != nil {
			return zero, false, err
		}
		return out, true, nil
	}
	if err != nil {
		return zero, false, err
	}
	out, err := fn(tx)
	if err != nil {
		return zero, false, err
	}
	body, err := json.Marshal(out)
	if err != nil {
		return zero, false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE "+table+" SET response=$4 WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3", r.Tenant, r.Operation, r.Key, body); err != nil {
		return zero, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, false, err
	}
	return out, false, nil
}
func Rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
