package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/jackc/pgx/v5"
)

var ErrOwner = ports.ErrOwner
var ErrStep = ports.ErrStep
var _ ports.TemporalStore = (*Store)(nil)

func (s *Store) TemporalClaim(ctx context.Context, in ports.TemporalInput) (ports.Job, *ports.TemporalResult, error) {
	var j ports.Job
	if !in.Execution.Valid() || in.Step < 0 {
		return j, nil, ErrStep
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return j, nil, err
	}
	defer cmdtx.Rollback(tx)
	var request, state []byte
	var parent, cancelMeta envelope.Metadata
	var paused bool
	var owner, status string
	var step int64
	err = tx.QueryRow(ctx, `SELECT request,state,metadata,cancel_metadata,cancel_requested,paused,engine,status,temporal_step FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, in.TenantID, in.ID).Scan(&request, &state, &parent, &cancelMeta, &j.CancelRequested, &paused, &owner, &status, &step)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, nil, ErrNotFound
	}
	if err != nil {
		return j, nil, err
	}
	if owner != "temporal" {
		return j, nil, ErrOwner
	}
	if err = json.Unmarshal(request, &j.Request); err != nil {
		return j, nil, err
	}
	if err = json.Unmarshal(state, &j.State); err != nil {
		return j, nil, err
	}
	if in.ForceCancel && !j.CancelRequested && !domain.Terminal(status) {
		workerCtx, err := envelope.Worker(ctx, parent, "checkout", in.ID)
		if err != nil {
			return j, nil, err
		}
		cancelMeta = envelope.From(workerCtx)
		meta, _ := json.Marshal(cancelMeta)
		if _, err = tx.Exec(ctx, `UPDATE checkout.jobs SET cancel_requested=true,cancel_metadata=$3 WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID, meta); err != nil {
			return j, nil, err
		}
		j.CancelRequested = true
	}
	j.Metadata = parent
	if j.CancelRequested && cancelMeta.Version == 1 {
		j.Metadata = cancelMeta
	}
	var cached ports.TemporalResult
	err = tx.QueryRow(ctx, `SELECT result FROM checkout.temporal_steps WHERE tenant_id=$1 AND id=$2::uuid AND step=$3`, in.TenantID, in.ID, in.Step).Scan(&cached)
	if err == nil {
		return j, &cached, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return j, nil, err
	}
	if domain.Terminal(status) {
		return j, &ports.TemporalResult{State: j.State}, tx.Commit(ctx)
	}
	if step != in.Step {
		return j, nil, ErrStep
	}
	if paused {
		return j, &ports.TemporalResult{State: j.State, Wait: "paused"}, tx.Commit(ctx)
	}
	err = tx.QueryRow(ctx, `UPDATE checkout.jobs SET lease_token=lease_token+1,lease_until=clock_timestamp()+interval '30 seconds',updated_at=now() WHERE tenant_id=$1 AND id=$2::uuid AND lease_until<=clock_timestamp() RETURNING lease_token`, in.TenantID, in.ID).Scan(&j.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, &ports.TemporalResult{State: j.State, Wait: "lease"}, tx.Commit(ctx)
	}
	if err != nil {
		return j, nil, err
	}
	return j, nil, tx.Commit(ctx)
}
func (s *Store) TemporalSave(ctx context.Context, j ports.Job, step int64, state domain.State, retry bool) (ports.TemporalResult, error) {
	result := ports.TemporalResult{State: state}
	if retry {
		result.State = j.State
		result.Wait = "retry"
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer cmdtx.Rollback(tx)
	var current int64
	err = tx.QueryRow(ctx, `SELECT temporal_step FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid AND engine='temporal' AND lease_token=$3 FOR UPDATE`, j.Request.TenantID, j.Request.ID, j.Token).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && current != step {
		return result, ErrLostLease
	}
	if err != nil {
		return result, err
	}
	advance := 1
	if retry {
		advance = 0
	}
	body, _ := json.Marshal(result.State)
	tag, err := tx.Exec(ctx, `UPDATE checkout.jobs SET state=$4,status=$5,temporal_step=temporal_step+$6,lease_until='-infinity',attempts=attempts+$7,last_error=$8,updated_at=now() WHERE tenant_id=$1 AND id=$2::uuid AND engine='temporal' AND NOT paused AND lease_token=$3 AND lease_until>clock_timestamp()`, j.Request.TenantID, j.Request.ID, j.Token, body, result.State.Stage, advance, boolInt(retry), errorCode(retry))
	if err != nil {
		return result, err
	}
	if tag.RowsAffected() != 1 {
		return result, ErrLostLease
	}
	if !retry {
		m, err := envelope.Record(ctx, "system:internal", j.Request.ID)
		if err != nil {
			return result, err
		}
		meta, _ := json.Marshal(m)
		encoded, _ := json.Marshal(result)
		if _, err = tx.Exec(ctx, `INSERT INTO checkout.temporal_steps(tenant_id,id,step,result,metadata) VALUES($1,$2::uuid,$3,$4,$5)`, j.Request.TenantID, j.Request.ID, step, encoded, meta); err != nil {
			return result, err
		}
	}
	return result, tx.Commit(ctx)
}

func (s *Store) StartTemporalOne(ctx context.Context, t ports.TemporalStarter) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer cmdtx.Rollback(tx)
	var e ports.Execution
	err = tx.QueryRow(ctx, `SELECT tenant_id,id::text FROM checkout.jobs WHERE engine='temporal' AND temporal_started_at IS NULL AND status NOT IN('confirmed','failed','cancelled') ORDER BY tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&e.TenantID, &e.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = t.Start(ctx, e); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE checkout.jobs SET temporal_started_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2::uuid`, e.TenantID, e.ID); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) WakeTemporalOne(ctx context.Context, t ports.TemporalStarter) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer cmdtx.Rollback(tx)
	var e ports.Execution
	var id, status string
	err = tx.QueryRow(ctx, `SELECT w.event_id::text,w.tenant_id,w.id::text,j.status FROM checkout.temporal_wakes w JOIN checkout.jobs j ON j.tenant_id=w.tenant_id AND j.id=w.id WHERE w.delivered_at IS NULL AND (j.temporal_started_at IS NOT NULL OR j.status IN('confirmed','failed','cancelled')) ORDER BY w.event_id FOR UPDATE OF w SKIP LOCKED LIMIT 1`).Scan(&id, &e.TenantID, &e.ID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !domain.Terminal(status) {
		if err = t.Wake(ctx, e); err != nil {
			return true, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE checkout.temporal_wakes SET delivered_at=clock_timestamp() WHERE event_id=$1::uuid`, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
