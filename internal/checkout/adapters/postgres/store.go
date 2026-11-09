package postgres

import (
	"context"
	"encoding/json"
	"errors"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
)

var ErrNotFound = errors.New("checkout not found")
var ErrLostLease = errors.New("checkout lease superseded or expired")
var ErrTerminal = errors.New("checkout already completed")

type Store struct{ Pool *pgxpool.Pool }

var _ ports.Store = (*Store)(nil)

func (s *Store) Submit(ctx context.Context, r orders.Requested) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	initial, _ := json.Marshal(domain.State{Stage: domain.Reserving})
	tag, err := s.Pool.Exec(ctx, `INSERT INTO checkout.jobs(tenant_id,id,order_id,request,state) VALUES($1,$2::uuid,$3::uuid,$4,$5) ON CONFLICT DO NOTHING`, r.TenantID, r.ID, r.OrderID, body, initial)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var existing []byte
	if err = s.Pool.QueryRow(ctx, `SELECT request FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, r.TenantID, r.ID).Scan(&existing); err != nil {
		return err
	}
	var old orders.Requested
	if err = json.Unmarshal(existing, &old); err != nil {
		return err
	}
	if !reflect.DeepEqual(old, r) {
		return orders.ErrConflict
	}
	return nil
}
func (s *Store) Claim(ctx context.Context) (ports.Job, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ports.Job{}, false, err
	}
	defer cmdtx.Rollback(tx)
	var job ports.Job
	var request, state []byte
	err = tx.QueryRow(ctx, `SELECT request,state,cancel_requested FROM checkout.jobs WHERE status NOT IN('confirmed','failed','cancelled') AND next_attempt_at<=clock_timestamp() AND lease_until<=clock_timestamp() ORDER BY next_attempt_at,tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&request, &state, &job.CancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, false, nil
	}
	if err != nil {
		return job, false, err
	}
	if err = json.Unmarshal(request, &job.Request); err != nil {
		return job, false, err
	}
	if err = json.Unmarshal(state, &job.State); err != nil {
		return job, false, err
	}
	err = tx.QueryRow(ctx, `UPDATE checkout.jobs SET lease_token=lease_token+1,lease_until=clock_timestamp()+interval '30 seconds',updated_at=now() WHERE tenant_id=$1 AND id=$2::uuid RETURNING lease_token`, job.Request.TenantID, job.Request.ID).Scan(&job.Token)
	if err != nil {
		return job, false, err
	}
	return job, true, tx.Commit(ctx)
}
func (s *Store) Save(ctx context.Context, j ports.Job, state domain.State, retry bool) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	delay := 0
	if retry {
		delay = 500
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE checkout.jobs SET state=$4,status=$5,lease_until='-infinity',next_attempt_at=clock_timestamp()+($6*interval '1 millisecond'),attempts=attempts+$7,last_error=$8,updated_at=now() WHERE tenant_id=$1 AND id=$2::uuid AND lease_token=$3 AND lease_until>clock_timestamp()`, j.Request.TenantID, j.Request.ID, j.Token, body, state.Stage, delay, boolInt(retry), errorCode(retry))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLostLease
	}
	return nil
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func errorCode(b bool) any {
	if b {
		return "step_retry"
	}
	return nil
}
func (s *Store) Get(ctx context.Context, tenant, id string) (ports.View, error) {
	var v ports.View
	var body, request []byte
	err := s.Pool.QueryRow(ctx, `SELECT id::text,order_id::text,status,state,request,attempts,cancel_requested,updated_at FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id).Scan(&v.ID, &v.OrderID, &v.Status, &body, &request, &v.Attempts, &v.CancelRequested, &v.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil {
		return v, err
	}
	var state domain.State
	var r orders.Requested
	if err = json.Unmarshal(body, &state); err != nil {
		return v, err
	}
	if err = json.Unmarshal(request, &r); err != nil {
		return v, err
	}
	v.Reason = state.Reason
	v.Currency = r.Quote.Currency
	v.TotalMinor = r.Quote.TotalMinor
	return v, nil
}
func (s *Store) Cancel(ctx context.Context, tenant, id string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE checkout.jobs SET cancel_requested=true,next_attempt_at=now() WHERE tenant_id=$1 AND id=$2::uuid AND status NOT IN('confirmed','failed','cancelled')`, tenant, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	v, err := s.Get(ctx, tenant, id)
	if err != nil {
		return err
	}
	if v.Status == domain.Cancelled {
		return nil
	}
	return ErrTerminal
}
