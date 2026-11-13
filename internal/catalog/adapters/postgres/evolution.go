package postgres

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrRolloutBusy = errors.New("schema rollout is busy")
var ErrRolloutNotReady = errors.New("schema rollout precondition is not satisfied")
var ErrRolloutRetired = errors.New("schema capability has been retired")
var rolloutIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

const rolloutLock int64 = 184502303

type BrandRolloutStatus struct {
	Phase           string     `json:"phase"`
	Pending         int64      `json:"pending"`
	Batches         int64      `json:"batches"`
	Rows            int64      `json:"rows_backfilled"`
	NextBatch       time.Time  `json:"next_batch_at"`
	RollbackUntil   *time.Time `json:"rollback_until"`
	RollbackRetired bool       `json:"rollback_retired"`
	LegacyReaders   int        `json:"unretired_legacy_readers"`
	LegacySessions  int        `json:"legacy_database_sessions"`
}
type BrandBatch struct {
	ID        string `json:"id"`
	Rows      int    `json:"rows"`
	Pending   bool   `json:"pending"`
	Replay    bool   `json:"replay"`
	Throttled bool   `json:"throttled"`
}

func (s *Store) BrandRollout(ctx context.Context) (BrandRolloutStatus, error) {
	var r BrandRolloutStatus
	e := s.Pool.QueryRow(ctx, `SELECT phase,(SELECT count(*) FROM catalog.brands WHERE display_name IS NULL),batches,rows_backfilled,next_batch_at,rollback_until,rollback_retired_at IS NOT NULL,
 (SELECT count(*) FROM catalog.brand_schema_readers WHERE capability=1 AND retired_at IS NULL),
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND application_name LIKE 'atlas-legacy/%') FROM catalog.brand_rollout WHERE id=1`).Scan(&r.Phase, &r.Pending, &r.Batches, &r.Rows, &r.NextBatch, &r.RollbackUntil, &r.RollbackRetired, &r.LegacyReaders, &r.LegacySessions)
	return r, e
}
func auditRollout(ctx context.Context, tx pgx.Tx, action, operator, reason string) error {
	if len(operator) < 1 || len(operator) > 100 || len(reason) < 1 || len(reason) > 200 {
		return ErrRolloutNotReady
	}
	_, e := tx.Exec(ctx, `INSERT INTO catalog.brand_rollout_audit(action,operator,reason) VALUES($1,$2,$3)`, action, operator, reason)
	return e
}
func (s *Store) withRolloutGate(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	c, e := s.Pool.Acquire(ctx)
	if e != nil {
		return e
	}
	defer c.Release()
	var acquired bool
	if e = c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, rolloutLock).Scan(&acquired); e != nil {
		return e
	}
	if !acquired {
		return ErrRolloutBusy
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, rolloutLock)
	}()
	return fn(c)
}

// PrepareBrandBackfill builds the owned partial index outside a transaction.
// An interrupted concurrent build leaves an invalid index, which is repaired
// before reporting readiness; IF NOT EXISTS alone would hide that failure.
func (s *Store) PrepareBrandBackfill(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		var valid bool
		var owned bool
		var expected bool
		e := c.QueryRow(ctx, `SELECT i.indisvalid,i.indrelid='catalog.brands'::regclass,COALESCE(NOT i.indisunique AND i.indnkeyatts=2 AND pg_get_indexdef(i.indexrelid,1,true)='tenant_id' AND pg_get_indexdef(i.indexrelid,2,true)='id' AND pg_get_expr(i.indpred,i.indrelid)='(display_name IS NULL)',false) FROM pg_index i WHERE i.indexrelid=to_regclass('catalog.brand_display_pending')`).Scan(&valid, &owned, &expected)
		if e == nil {
			if !owned || !expected {
				return ErrRolloutNotReady
			}
			if valid {
				return nil
			}
			if _, e = c.Exec(ctx, `DROP INDEX CONCURRENTLY catalog.brand_display_pending`); e != nil {
				return e
			}
		} else if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		_, e = c.Exec(ctx, `CREATE INDEX CONCURRENTLY brand_display_pending ON catalog.brands(tenant_id,id) WHERE display_name IS NULL`)
		return e
	})
}

// BackfillBrandNames reselects the pending predicate after every commit. It
// deliberately does not advance a keyset cursor past SKIP LOCKED rows.
// The batch receipt and progress commit with the data, so an uncertain commit
// can be resolved by replaying the same ID. Business revisions/history stay put.
func (s *Store) BackfillBrandNames(parent context.Context, id string, limit int, interval time.Duration) (BrandBatch, error) {
	r := BrandBatch{ID: id}
	if !rolloutIdentity.MatchString(id) || limit < 1 || limit > 128 || interval < 10*time.Millisecond || interval > time.Minute {
		return r, ErrRolloutNotReady
	}
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return r, e
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SET LOCAL statement_timeout='300ms'; SET LOCAL lock_timeout='100ms'; SET LOCAL idle_in_transaction_session_timeout='300ms'`); e != nil {
		return r, e
	}
	var locked bool
	if e = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, rolloutLock).Scan(&locked); e != nil {
		return r, e
	}
	if !locked {
		return r, ErrRolloutBusy
	}
	var recordedLimit int
	var recordedInterval int64
	e = tx.QueryRow(ctx, `SELECT rows_backfilled,pending,batch_limit,min_interval_ms FROM catalog.brand_backfill_receipts WHERE batch_id=$1`, id).Scan(&r.Rows, &r.Pending, &recordedLimit, &recordedInterval)
	if e == nil {
		if recordedLimit != limit || recordedInterval != interval.Milliseconds() {
			return r, ErrRolloutNotReady
		}
		r.Replay = true
		return r, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return r, e
	}
	var phase string
	var due, indexReady bool
	if e = tx.QueryRow(ctx, `SELECT phase,clock_timestamp()>=next_batch_at,COALESCE((SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('catalog.brand_display_pending')),false) FROM catalog.brand_rollout WHERE id=1 FOR UPDATE`).Scan(&phase, &due, &indexReady); e != nil {
		return r, e
	}
	if phase == "validated" || phase == "contracted" {
		return r, ErrRolloutRetired
	}
	if !indexReady {
		return r, ErrRolloutNotReady
	}
	if !due {
		r.Throttled = true
		return r, tx.Commit(ctx)
	}
	if e = tx.QueryRow(ctx, `WITH selected AS (SELECT tenant_id,id FROM catalog.brands WHERE display_name IS NULL ORDER BY tenant_id,id LIMIT $1 FOR UPDATE SKIP LOCKED), changed AS (UPDATE catalog.brands b SET display_name=b.name FROM selected x WHERE b.tenant_id=x.tenant_id AND b.id=x.id AND b.display_name IS NULL RETURNING 1) SELECT count(*) FROM changed`, limit).Scan(&r.Rows); e != nil {
		return r, e
	}
	if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM catalog.brands WHERE display_name IS NULL)`).Scan(&r.Pending); e != nil {
		return r, e
	}
	if _, e = tx.Exec(ctx, `UPDATE catalog.brand_rollout SET batches=batches+1,rows_backfilled=rows_backfilled+$1,next_batch_at=clock_timestamp()+$2::bigint*interval '1 millisecond',phase=CASE WHEN NOT $3::boolean THEN 'backfilled' ELSE 'expanded' END WHERE id=1`, r.Rows, interval.Milliseconds(), r.Pending); e != nil {
		return r, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO catalog.brand_backfill_receipts(batch_id,rows_backfilled,pending,batch_limit,min_interval_ms) VALUES($1,$2,$3,$4,$5)`, id, r.Rows, r.Pending, limit, interval.Milliseconds()); e != nil {
		return r, e
	}
	if e = tx.Commit(ctx); e != nil {
		return BrandBatch{ID: id}, e
	}
	return r, nil
}

// ValidateBrandNames commits the fast NOT VALID metadata step before scanning.
// Holding the initial ACCESS EXCLUSIVE lock during VALIDATE would block writers.
func (s *Store) ValidateBrandNames(ctx context.Context, operator, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		var phase string
		var pending int64
		if e := c.QueryRow(ctx, `SELECT phase,(SELECT count(*) FROM catalog.brands WHERE display_name IS NULL) FROM catalog.brand_rollout WHERE id=1`).Scan(&phase, &pending); e != nil {
			return e
		}
		if phase == "contracted" {
			return ErrRolloutRetired
		}
		if pending != 0 {
			return ErrRolloutNotReady
		}
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='250ms'; SET LOCAL statement_timeout='1s'; SET LOCAL idle_in_transaction_session_timeout='300ms'`); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `DO $validation$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='catalog.brands'::regclass AND conname='brand_display_nn') THEN ALTER TABLE catalog.brands ADD CONSTRAINT brand_display_nn CHECK(display_name IS NOT NULL) NOT VALID; END IF; END $validation$`); e != nil {
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		// A separate transaction uses SHARE UPDATE EXCLUSIVE, compatible with DML.
		tx, e = c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='250ms'; SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='300ms'`); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `ALTER TABLE catalog.brands VALIDATE CONSTRAINT brand_display_nn`); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE catalog.brand_rollout SET phase='validated' WHERE id=1`); e != nil {
			return e
		}
		if e = auditRollout(ctx, tx, "validated", operator, reason); e != nil {
			return e
		}
		return tx.Commit(ctx)
	})
}

func (s *Store) LeaseBrandReader(ctx context.Context, id string, capability int, ttl time.Duration) error {
	if !rolloutIdentity.MatchString(id) || (capability != 1 && capability != 2) || ttl < 100*time.Millisecond || ttl > time.Hour {
		return ErrRolloutNotReady
	}
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		var phase string
		if e := c.QueryRow(ctx, `SELECT phase FROM catalog.brand_rollout WHERE id=1`).Scan(&phase); e != nil {
			return e
		}
		if phase == "contracted" && capability < 2 {
			return ErrRolloutRetired
		}
		tag, e := c.Exec(ctx, `INSERT INTO catalog.brand_schema_readers(id,capability,lease_until) VALUES($1,$2,clock_timestamp()+$3::bigint*interval '1 millisecond') ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until WHERE catalog.brand_schema_readers.retired_at IS NULL AND catalog.brand_schema_readers.capability=excluded.capability`, id, capability, ttl.Milliseconds())
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return ErrRolloutRetired
		}
		return nil
	})
}
func (s *Store) RetireBrandReader(ctx context.Context, id, operator, reason string) error {
	if !rolloutIdentity.MatchString(id) {
		return ErrRolloutNotReady
	}
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		var expired, active bool
		if e = tx.QueryRow(ctx, `SELECT lease_until<=clock_timestamp(),EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND application_name='atlas-legacy/'||$1) FROM catalog.brand_schema_readers WHERE id=$1 FOR UPDATE`, id).Scan(&expired, &active); e != nil {
			return e
		}
		if !expired || active {
			return ErrRolloutNotReady
		}
		if _, e = tx.Exec(ctx, `UPDATE catalog.brand_schema_readers SET retired_at=COALESCE(retired_at,clock_timestamp()) WHERE id=$1`, id); e != nil {
			return e
		}
		if e = auditRollout(ctx, tx, "reader-retired", operator, reason); e != nil {
			return e
		}
		return tx.Commit(ctx)
	})
}
func (s *Store) OpenBrandRollbackWindow(ctx context.Context, duration time.Duration, operator, reason string) error {
	if duration < time.Second || duration > 30*24*time.Hour {
		return ErrRolloutNotReady
	}
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		tag, e := tx.Exec(ctx, `UPDATE catalog.brand_rollout SET rollback_until=GREATEST(COALESCE(rollback_until,'-infinity'),clock_timestamp()+$1::bigint*interval '1 millisecond') WHERE id=1 AND phase<>'contracted' AND rollback_retired_at IS NULL`, duration.Milliseconds())
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return ErrRolloutRetired
		}
		if e = auditRollout(ctx, tx, "rollback-window", operator, reason); e != nil {
			return e
		}
		return tx.Commit(ctx)
	})
}
func rolloutReady(ctx context.Context, tx pgx.Tx, requireRetired bool) error {
	var phase string
	var expired, retired bool
	var readers, sessions int64
	e := tx.QueryRow(ctx, `SELECT phase,COALESCE(rollback_until<=clock_timestamp(),false),rollback_retired_at IS NOT NULL,
 (SELECT count(*) FROM catalog.brand_schema_readers WHERE capability=1 AND retired_at IS NULL),
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND application_name LIKE 'atlas-legacy/%') FROM catalog.brand_rollout WHERE id=1 FOR UPDATE`).Scan(&phase, &expired, &retired, &readers, &sessions)
	if e != nil {
		return e
	}
	if phase != "validated" || !expired || readers != 0 || sessions != 0 || requireRetired && !retired {
		return ErrRolloutNotReady
	}
	return nil
}
func (s *Store) RetireBrandRollback(ctx context.Context, operator, reason string) error {
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		if e = rolloutReady(ctx, tx, false); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE catalog.brand_rollout SET rollback_retired_at=clock_timestamp() WHERE id=1`); e != nil {
			return e
		}
		if e = auditRollout(ctx, tx, "rollback-retired", operator, reason); e != nil {
			return e
		}
		return tx.Commit(ctx)
	})
}
func (s *Store) ContractBrandNames(parent context.Context, operator, reason string) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	return s.withRolloutGate(ctx, func(c *pgxpool.Conn) error {
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(context.Background())
		if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='250ms'; SET LOCAL statement_timeout='1s'; SET LOCAL idle_in_transaction_session_timeout='300ms'`); e != nil {
			return e
		}
		if e = rolloutReady(ctx, tx, true); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `LOCK TABLE catalog.brands IN ACCESS EXCLUSIVE MODE`); e != nil {
			return e
		}
		// Replace the read view before removing its old-column dependency.
		if _, e = tx.Exec(ctx, `CREATE OR REPLACE VIEW catalog.brand_names AS SELECT tenant_id,id,COALESCE(display_name,'') AS display_name,version FROM catalog.brands; DROP TRIGGER bridge_brand_names ON catalog.brands; ALTER TABLE catalog.brands ALTER COLUMN display_name SET NOT NULL; ALTER TABLE catalog.brands DROP COLUMN name; UPDATE catalog.brand_rollout SET phase='contracted' WHERE id=1`); e != nil {
			return e
		}
		if e = auditRollout(ctx, tx, "contracted", operator, reason); e != nil {
			return e
		}
		return tx.Commit(ctx)
	})
}
