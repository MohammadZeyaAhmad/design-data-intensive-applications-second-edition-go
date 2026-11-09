package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

var _ ports.Store = (*Store)(nil)

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) Execute(ctx context.Context, r ports.Request, fn func(context.Context, ports.Transaction) (ports.Outcome, error)) (ports.Outcome, bool, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ports.Outcome{}, false, err
	}
	defer rollback(tx)
	var key string
	err = tx.QueryRow(ctx, `INSERT INTO inventory.command_results (tenant_id,operation,idempotency_key,request_hash) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING idempotency_key`, r.TenantID, r.Operation, r.Key, r.Hash[:]).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		var hash, body []byte
		// This is a new READ COMMITTED statement after the conflicting insert
		// has finished, so it observes the winning transaction's saved response.
		if err = tx.QueryRow(ctx, `SELECT request_hash,response FROM inventory.command_results WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3`, r.TenantID, r.Operation, r.Key).Scan(&hash, &body); err != nil {
			return ports.Outcome{}, false, err
		}
		if !bytes.Equal(hash, r.Hash[:]) {
			return ports.Outcome{}, false, domain.ErrIdempotencyConflict
		}
		if len(body) == 0 {
			return ports.Outcome{}, false, fmt.Errorf("committed idempotency response missing")
		}
		var out ports.Outcome
		if err = json.Unmarshal(body, &out); err != nil {
			return ports.Outcome{}, false, err
		}
		return out, true, nil
	}
	if err != nil {
		return ports.Outcome{}, false, err
	}
	out, err := fn(ctx, &transaction{tx: tx, tenant: r.TenantID})
	if err != nil {
		return ports.Outcome{}, false, err
	}
	body, err := json.Marshal(out)
	if err != nil {
		return ports.Outcome{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE inventory.command_results SET response=$4 WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3`, r.TenantID, r.Operation, r.Key, body); err != nil {
		return ports.Outcome{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ports.Outcome{}, false, err
	}
	return out, false, nil
}

func scanStock(row pgx.Row) (domain.StockState, error) {
	var s domain.StockState
	err := row.Scan(&s.TenantID, &s.SKU, &s.OnHand, &s.Reserved, &s.Allocated, &s.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, domain.ErrNotFound
	}
	if err != nil {
		return s, err
	}
	if _, err = domain.RestoreStock(s); err != nil {
		return s, err
	}
	return s, nil
}

func (s *Store) Stock(ctx context.Context, tenant, sku string) (domain.StockState, error) {
	return scanStock(s.Pool.QueryRow(ctx, `SELECT tenant_id,sku,on_hand,reserved,allocated,version FROM inventory.stock WHERE tenant_id=$1 AND sku=$2`, tenant, sku))
}
func (s *Store) Projection(ctx context.Context, tenant, sku string) (domain.StockState, error) {
	return scanStock(s.Pool.QueryRow(ctx, `SELECT tenant_id,sku,on_hand,reserved,allocated,version FROM inventory.stock_views WHERE tenant_id=$1 AND sku=$2`, tenant, sku))
}

func scanReservation(row pgx.Row) (domain.ReservationState, error) {
	var r domain.ReservationState
	err := row.Scan(&r.ID, &r.TenantID, &r.SKU, &r.Quantity, &r.Status, &r.Version, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if _, err = domain.RestoreReservation(r); err != nil {
		return r, err
	}
	return r, nil
}
func (s *Store) Reservation(ctx context.Context, tenant, id string) (domain.ReservationState, error) {
	return scanReservation(s.Pool.QueryRow(ctx, `SELECT id::text,tenant_id,sku,quantity,status,version,expires_at FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id))
}

type transaction struct {
	tx     pgx.Tx
	tenant string
}

func (t *transaction) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := t.tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now.UTC(), err
}

func (t *transaction) EnsureStock(ctx context.Context, sku string) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO inventory.stock (tenant_id,sku) VALUES ($1,$2) ON CONFLICT DO NOTHING`, t.tenant, sku)
	return err
}
func (t *transaction) LockStock(ctx context.Context, sku string) (*domain.Stock, error) {
	s, err := scanStock(t.tx.QueryRow(ctx, `SELECT tenant_id,sku,on_hand,reserved,allocated,version FROM inventory.stock WHERE tenant_id=$1 AND sku=$2 FOR UPDATE`, t.tenant, sku))
	if err != nil {
		return nil, err
	}
	return domain.RestoreStock(s)
}
func (t *transaction) SaveStock(ctx context.Context, aggregate *domain.Stock) error {
	s := aggregate.Snapshot()
	if s.TenantID != t.tenant {
		return domain.ErrCorruptState
	}
	tag, err := t.tx.Exec(ctx, `UPDATE inventory.stock SET on_hand=$3,reserved=$4,allocated=$5,version=$6 WHERE tenant_id=$1 AND sku=$2 AND version=$6-1`, t.tenant, s.SKU, s.OnHand, s.Reserved, s.Allocated, s.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	return nil
}
func (t *transaction) LockReservation(ctx context.Context, id string) (*domain.Reservation, error) {
	r, err := scanReservation(t.tx.QueryRow(ctx, `SELECT id::text,tenant_id,sku,quantity,status,version,expires_at FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, t.tenant, id))
	if err != nil {
		return nil, err
	}
	return domain.RestoreReservation(r)
}
func (t *transaction) InsertReservation(ctx context.Context, aggregate *domain.Reservation) error {
	r := aggregate.Snapshot()
	if r.TenantID != t.tenant {
		return domain.ErrCorruptState
	}
	_, err := t.tx.Exec(ctx, `INSERT INTO inventory.reservations (tenant_id,id,sku,quantity,status,version,expires_at) VALUES ($1,$2::uuid,$3,$4,$5,$6,$7)`, t.tenant, r.ID, r.SKU, r.Quantity, r.Status, r.Version, r.ExpiresAt)
	return err
}
func (t *transaction) SaveReservation(ctx context.Context, aggregate *domain.Reservation) error {
	r := aggregate.Snapshot()
	if r.TenantID != t.tenant {
		return domain.ErrCorruptState
	}
	tag, err := t.tx.Exec(ctx, `UPDATE inventory.reservations SET status=$3,version=$4 WHERE tenant_id=$1 AND id=$2::uuid AND version=$4-1`, t.tenant, r.ID, r.Status, r.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	return nil
}
func (t *transaction) AppendEvent(ctx context.Context, eventType string, out ports.Outcome) error {
	if out.Stock.TenantID != t.tenant {
		return domain.ErrCorruptState
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO inventory.outbox (tenant_id,sku,aggregate_version,event_type,payload) VALUES ($1,$2,$3,$4,$5)`, t.tenant, out.Stock.SKU, out.Stock.Version, eventType, b)
	return err
}
