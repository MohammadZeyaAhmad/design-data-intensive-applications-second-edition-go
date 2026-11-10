// Package workcontrol supplies an opt-in scheduling primitive for owner adapters.
// It cannot modify workflow JSON, payment identity, allocation IDs or checkpoints.
package workcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/operator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// All identifiers/policies are constants supplied by the owning context.
type Store struct {
	Pool                *pgxpool.Pool
	Schema, Table, Kind string
	Terminal            []string
	RetryStatus         string
}

func (s Store) table() string { return pgx.Identifier{s.Schema, s.Table}.Sanitize() }
func (s Store) audit() string { return pgx.Identifier{s.Schema, "control_audit"}.Sanitize() }
func scan(row pgx.Row, kind string) (api.View, error) {
	v := api.View{Kind: kind}
	err := row.Scan(&v.TenantID, &v.ID, &v.Status, &v.Paused, &v.ControlVersion, &v.LeaseToken, &v.LeaseActive, &v.Attempts, &v.LastError, &v.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, api.ErrNotFound
	}
	return v, err
}

const columns = "tenant_id,id::text,status,paused,control_version,lease_token,lease_until>clock_timestamp(),attempts,coalesce(last_error,''),metadata"

func (s Store) Inspect(ctx context.Context, tenant, id string) (api.View, error) {
	if err := operator.Require(ctx, tenant); err != nil {
		return api.View{}, err
	}
	if !identity.ValidID(id) {
		return api.View{}, api.ErrInvalid
	}
	return scan(s.Pool.QueryRow(ctx, "SELECT "+columns+" FROM "+s.table()+" WHERE tenant_id=$1 AND id=$2::uuid", tenant, id), s.Kind)
}
func (s Store) Control(ctx context.Context, c api.Command) (api.Result, bool, error) {
	if err := operator.Require(ctx, c.TenantID); err != nil {
		return api.Result{}, false, err
	}
	if !identity.ValidID(c.ID) || c.ExpectedVersion < 0 || !slices.Contains([]string{"pause", "resume", "retry"}, c.Action) || !slices.Contains([]string{"investigation", "dependency_restored", "operator_retry"}, c.Reason) {
		return api.Result{}, false, api.ErrInvalid
	}
	r, err := commands.New(c.TenantID, "control-"+c.Action, c.Key, c)
	if err != nil {
		return api.Result{}, false, err
	}
	return cmdtx.Execute(ctx, s.Pool, s.Schema, r, func(tx pgx.Tx) (api.Result, error) {
		before, err := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM "+s.table()+" WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE", c.TenantID, c.ID), s.Kind)
		if err != nil {
			return api.Result{}, err
		}
		if before.ControlVersion != c.ExpectedVersion {
			return api.Result{}, api.ErrConflict
		}
		if slices.Contains(s.Terminal, before.Status) {
			return api.Result{}, api.ErrTerminal
		}
		// The first read can wait on a row lock; re-evaluate physical time now.
		if err = tx.QueryRow(ctx, "SELECT lease_until>clock_timestamp() FROM "+s.table()+" WHERE tenant_id=$1 AND id=$2::uuid", c.TenantID, c.ID).Scan(&before.LeaseActive); err != nil {
			return api.Result{}, err
		}
		if before.LeaseActive {
			return api.Result{}, api.ErrBusy
		}
		if before.ControlVersion == math.MaxInt64 || before.LeaseToken == math.MaxInt64 {
			return api.Result{}, api.ErrConflict
		}
		after := before
		switch c.Action {
		case "pause":
			if before.Paused {
				return api.Result{}, api.ErrConflict
			}
			after.Paused = true
		case "resume":
			if !before.Paused {
				return api.Result{}, api.ErrConflict
			}
			after.Paused = false
		case "retry":
			if before.Paused || s.RetryStatus != "" && before.Status != s.RetryStatus {
				return api.Result{}, api.ErrConflict
			}
		}
		after.ControlVersion++
		after.LeaseToken++
		after.LeaseActive = false
		id, err := identity.NewID()
		if err != nil {
			return api.Result{}, err
		}
		m := envelope.From(ctx)
		if m.Version == 1 {
			m.Actor = "operator:local"
		}
		m, err = envelope.Record(envelope.With(ctx, m), "operator:local", id)
		if err != nil {
			return api.Result{}, err
		}
		old, _ := json.Marshal(before)
		next, _ := json.Marshal(after)
		meta, _ := json.Marshal(m)
		if _, err = tx.Exec(ctx, "UPDATE "+s.table()+" SET paused=$3,control_version=$4,lease_token=$5,lease_until='-infinity',next_attempt_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2::uuid", c.TenantID, c.ID, after.Paused, after.ControlVersion, after.LeaseToken); err != nil {
			return api.Result{}, err
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+s.audit()+" (tenant_id,id,version,audit_id,action,reason,before_state,after_state,metadata) VALUES($1,$2::uuid,$3,$4::uuid,$5,$6,$7,$8,$9)", c.TenantID, c.ID, after.ControlVersion, id, c.Action, c.Reason, old, next, meta)
		return api.Result{View: after, AuditID: id}, err
	})
}
func (s Store) ControlHistory(ctx context.Context, tenant, id string, after int64, limit int) (api.Page, error) {
	page := api.Page{Entries: []api.Audit{}, NextAfterVersion: after}
	if after < 0 || limit < 1 || limit > 100 {
		return page, api.ErrInvalid
	}
	if _, err := s.Inspect(ctx, tenant, id); err != nil {
		return page, err
	}
	rows, err := s.Pool.Query(ctx, "SELECT audit_id::text,version,action,reason,before_state,after_state,metadata,recorded_at FROM "+s.audit()+" WHERE tenant_id=$1 AND id=$2::uuid AND version>$3 ORDER BY version LIMIT $4", tenant, id, after, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry api.Audit
		if err = rows.Scan(&entry.ID, &entry.Version, &entry.Action, &entry.Reason, &entry.Before, &entry.After, &entry.Metadata, &entry.RecordedAt); err != nil {
			return page, err
		}
		page.Entries = append(page.Entries, entry)
		page.NextAfterVersion = entry.Version
	}
	return page, rows.Err()
}
