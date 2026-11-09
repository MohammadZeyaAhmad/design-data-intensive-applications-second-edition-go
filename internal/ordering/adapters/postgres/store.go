package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
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

func (s *Store) Execute(ctx context.Context, r ports.Request, id string, expected int64, decide func(*domain.Order) ([]domain.Event, error)) (domain.State, bool, error) {
	if !domain.ValidID(id) || expected < 0 {
		return domain.State{}, false, domain.ErrInvalidInput
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.State{}, false, err
	}
	defer rollback(tx)
	var commandID string
	err = tx.QueryRow(ctx, `INSERT INTO ordering.command_results (tenant_id,operation,idempotency_key,request_hash) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING command_id::text`, r.Tenant, r.Operation, r.Key, r.Hash[:]).Scan(&commandID)
	if errors.Is(err, pgx.ErrNoRows) {
		var hash, body []byte
		if err = tx.QueryRow(ctx, `SELECT request_hash,response FROM ordering.command_results WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3`, r.Tenant, r.Operation, r.Key).Scan(&hash, &body); err != nil {
			return domain.State{}, false, err
		}
		if !bytes.Equal(hash, r.Hash[:]) {
			return domain.State{}, false, domain.ErrIdempotencyConflict
		}
		var state domain.State
		if err = json.Unmarshal(body, &state); err != nil {
			return domain.State{}, false, domain.ErrInvalidEvent
		}
		if _, err = domain.Restore(state); err != nil {
			return domain.State{}, false, err
		}
		return state, true, nil
	}
	if err != nil {
		return domain.State{}, false, err
	}
	if expected == 0 {
		if _, err = tx.Exec(ctx, `INSERT INTO ordering.streams (tenant_id,id) VALUES ($1,$2::uuid) ON CONFLICT DO NOTHING`, r.Tenant, id); err != nil {
			return domain.State{}, false, err
		}
	}
	var head int64
	err = tx.QueryRow(ctx, `SELECT version FROM ordering.streams WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, r.Tenant, id).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.State{}, false, domain.ErrNotFound
	}
	if err != nil {
		return domain.State{}, false, err
	}
	if head != expected {
		return domain.State{}, false, domain.ErrConflict
	}
	order, err := loadAggregate(ctx, tx, r.Tenant, id, head, true)
	if err != nil {
		return domain.State{}, false, err
	}
	events, err := decide(order)
	if err != nil {
		return domain.State{}, false, err
	}
	if len(events) < 1 || len(events) > 64 {
		return domain.State{}, false, domain.ErrInvalidEvent
	}
	for _, event := range events {
		if err = order.Apply(event); err != nil {
			return domain.State{}, false, err
		}
		var eventID string
		err = tx.QueryRow(ctx, `INSERT INTO ordering.events (tenant_id,order_id,version,command_id,event_type,schema_version,payload) VALUES ($1,$2::uuid,$3,$4::uuid,$5,$6,$7) RETURNING event_id::text`, r.Tenant, id, event.Version, commandID, event.Type, event.SchemaVersion, []byte(event.Data)).Scan(&eventID)
		if err != nil {
			return domain.State{}, false, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ordering.outbox (tenant_id,order_id,version,event_id) VALUES ($1,$2::uuid,$3,$4::uuid)`, r.Tenant, id, event.Version, eventID); err != nil {
			return domain.State{}, false, err
		}
	}
	state := order.Snapshot()
	tag, err := tx.Exec(ctx, `UPDATE ordering.streams SET version=$3 WHERE tenant_id=$1 AND id=$2::uuid AND version=$4`, r.Tenant, id, state.Version, expected)
	if err != nil {
		return domain.State{}, false, err
	}
	if tag.RowsAffected() != 1 {
		return domain.State{}, false, domain.ErrConflict
	}
	body, err := json.Marshal(state)
	if err != nil {
		return domain.State{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.command_results SET response=$4 WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3`, r.Tenant, r.Operation, r.Key, body); err != nil {
		return domain.State{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.State{}, false, err
	}
	return state, false, nil
}

func loadAggregate(ctx context.Context, tx pgx.Tx, tenant, id string, head int64, useSnapshot bool) (*domain.Order, error) {
	order, err := domain.New(tenant, id)
	if err != nil {
		return nil, err
	}
	if useSnapshot && head > 0 {
		var version int64
		var schema int
		var body, checksum []byte
		err = tx.QueryRow(ctx, `SELECT version,schema_version,body,checksum FROM ordering.snapshots WHERE tenant_id=$1 AND order_id=$2::uuid`, tenant, id).Scan(&version, &schema, &body, &checksum)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if err == nil && schema == 1 && version > 0 && version <= head {
			var state domain.State
			if json.Unmarshal(body, &state) == nil && state.TenantID == tenant && strings.EqualFold(state.ID, id) && state.Version == version {
				// PostgreSQL JSONB normalizes formatting. Hash the typed canonical
				// representation so JSON key order cannot invalidate a good snapshot.
				canonical, _ := json.Marshal(state)
				sum := sha256.Sum256(canonical)
				if bytes.Equal(sum[:], checksum) {
					if restored, restoreErr := domain.Restore(state); restoreErr == nil {
						order = restored
					}
				}
			}
		}
	}
	rows, err := tx.Query(ctx, `SELECT version,event_type,schema_version,payload FROM ordering.events WHERE tenant_id=$1 AND order_id=$2::uuid AND version>$3 AND version<=$4 ORDER BY version`, tenant, id, order.Snapshot().Version, head)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var event domain.Event
		if err = rows.Scan(&event.Version, &event.Type, &event.SchemaVersion, &event.Data); err != nil {
			return nil, err
		}
		if err = order.Apply(event); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if order.Snapshot().Version != head {
		return nil, domain.ErrInvalidEvent
	}
	return order, nil
}

func (s *Store) read(ctx context.Context, tenant, id string, useSnapshot bool) (domain.State, error) {
	if !domain.ValidTenant(tenant) || !domain.ValidID(id) {
		return domain.State{}, domain.ErrInvalidInput
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.State{}, err
	}
	defer rollback(tx)
	var head int64
	err = tx.QueryRow(ctx, `SELECT version FROM ordering.streams WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.State{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.State{}, err
	}
	if head < 1 {
		return domain.State{}, domain.ErrInvalidEvent
	}
	order, err := loadAggregate(ctx, tx, tenant, id, head, useSnapshot)
	if err != nil {
		return domain.State{}, err
	}
	return order.Snapshot(), tx.Commit(ctx)
}
func (s *Store) Load(ctx context.Context, tenant, id string) (domain.State, error) {
	return s.read(ctx, tenant, id, true)
}
func (s *Store) Replay(ctx context.Context, tenant, id string) (domain.State, error) {
	return s.read(ctx, tenant, id, false)
}

func (s *Store) Projection(ctx context.Context, tenant, id string) (domain.State, error) {
	var body []byte
	var version int64
	err := s.Pool.QueryRow(ctx, `SELECT version,body FROM ordering.order_views WHERE tenant_id=$1 AND order_id=$2::uuid`, tenant, id).Scan(&version, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.State{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.State{}, err
	}
	var state domain.State
	if err = json.Unmarshal(body, &state); err != nil {
		return domain.State{}, domain.ErrInvalidEvent
	}
	if state.Version != version || state.TenantID != tenant || !strings.EqualFold(state.ID, id) {
		return domain.State{}, domain.ErrInvalidEvent
	}
	if _, err = domain.Restore(state); err != nil {
		return domain.State{}, err
	}
	return state, nil
}

func (s *Store) Events(ctx context.Context, tenant, id string, after int64, limit int) (ports.EventPage, error) {
	page := ports.EventPage{Events: []ports.EventRecord{}, NextAfterVersion: after}
	if !domain.ValidTenant(tenant) || !domain.ValidID(id) || after < 0 || limit < 1 || limit > 100 {
		return page, domain.ErrInvalidInput
	}
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ordering.streams WHERE tenant_id=$1 AND id=$2::uuid)`, tenant, id).Scan(&exists); err != nil {
		return page, err
	}
	if !exists {
		return page, domain.ErrNotFound
	}
	rows, err := s.Pool.Query(ctx, `SELECT event_id::text,version,event_type,schema_version,payload,recorded_at FROM ordering.events WHERE tenant_id=$1 AND order_id=$2::uuid AND version>$3 ORDER BY version LIMIT $4`, tenant, id, after, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var e ports.EventRecord
		if err = rows.Scan(&e.ID, &e.Version, &e.Type, &e.SchemaVersion, &e.Data, &e.RecordedAt); err != nil {
			return page, err
		}
		page.Events = append(page.Events, e)
		page.NextAfterVersion = e.Version
	}
	return page, rows.Err()
}

func (s *Store) Snapshot(ctx context.Context, tenant, id string) (domain.State, error) {
	state, err := s.Replay(ctx, tenant, id)
	if err != nil {
		return domain.State{}, err
	}
	body, err := json.Marshal(state)
	if err != nil {
		return domain.State{}, err
	}
	checksum := sha256.Sum256(body)
	_, err = s.Pool.Exec(ctx, `INSERT INTO ordering.snapshots (tenant_id,order_id,version,schema_version,body,checksum) VALUES ($1,$2::uuid,$3,1,$4,$5) ON CONFLICT (tenant_id,order_id) DO UPDATE SET version=EXCLUDED.version,schema_version=EXCLUDED.schema_version,body=EXCLUDED.body,checksum=EXCLUDED.checksum WHERE ordering.snapshots.version<=EXCLUDED.version`, tenant, id, state.Version, body, checksum[:])
	return state, err
}

func projectionLock(ctx context.Context, tx pgx.Tx, tenant, id string, wait bool) (bool, error) {
	key := "ordering/projection/" + tenant + "/" + strings.ToLower(id)
	if wait {
		_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,73920185))`, key)
		return err == nil, err
	}
	var acquired bool
	err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,73920185))`, key).Scan(&acquired)
	return acquired, err
}

// Rebuild replaces one stream's view under a stream-local barrier. It never
// treats a global sequence or wall timestamp as commit order.
func (s *Store) Rebuild(ctx context.Context, tenant, id string) (domain.State, error) {
	if !domain.ValidTenant(tenant) || !domain.ValidID(id) {
		return domain.State{}, domain.ErrInvalidInput
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.State{}, err
	}
	defer rollback(tx)
	if _, err = projectionLock(ctx, tx, tenant, id, true); err != nil {
		return domain.State{}, err
	}
	var head int64
	err = tx.QueryRow(ctx, `SELECT version FROM ordering.streams WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, tenant, id).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.State{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.State{}, err
	}
	order, err := loadAggregate(ctx, tx, tenant, id, head, false)
	if err != nil {
		return domain.State{}, err
	}
	state := order.Snapshot()
	if err = saveView(ctx, tx, state); err != nil {
		return domain.State{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ordering.projection_receipts (tenant_id,event_id) SELECT tenant_id,event_id FROM ordering.events WHERE tenant_id=$1 AND order_id=$2::uuid AND version<=$3 ON CONFLICT DO NOTHING`, tenant, id, head); err != nil {
		return domain.State{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.outbox SET processed_at=now(),quarantined=false WHERE tenant_id=$1 AND order_id=$2::uuid AND version<=$3`, tenant, id, head); err != nil {
		return domain.State{}, err
	}
	return state, tx.Commit(ctx)
}

func saveView(ctx context.Context, tx pgx.Tx, state domain.State) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ordering.order_views (tenant_id,order_id,version,body) VALUES ($1,$2::uuid,$3,$4) ON CONFLICT (tenant_id,order_id) DO UPDATE SET version=EXCLUDED.version,body=EXCLUDED.body`, state.TenantID, state.ID, state.Version, body)
	return err
}
