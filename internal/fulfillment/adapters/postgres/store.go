package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrLostLease = errors.New("shipment lease superseded or expired")

type Store struct{ Pool *pgxpool.Pool }

var _ ports.Store = (*Store)(nil)

func restore(body []byte) (*domain.Shipment, error) {
	var state domain.State
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, err
	}
	return domain.Restore(state)
}
func event(ctx context.Context, tx pgx.Tx, state domain.State, action string) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	cause := envelope.From(ctx).CausationID
	if cause == "" {
		cause = state.ID
	}
	m, err := envelope.Record(ctx, "system:internal", cause)
	if err != nil {
		return err
	}
	meta, _ := json.Marshal(m)
	_, err = tx.Exec(ctx, `INSERT INTO fulfillment.events(tenant_id,shipment_id,version,action,state,metadata) VALUES($1,$2::uuid,$3,$4,$5,$6)`, state.TenantID, state.ID, state.Version, action, body, meta)
	return err
}

func (s *Store) Submit(ctx context.Context, r api.Request) error {
	if !r.Metadata.Valid() || !domain.ValidID(r.ID) || !domain.ValidID(r.OrderID) || len(r.Allocations) < 1 || len(r.Allocations) > 100 {
		return domain.ErrInvalid
	}
	groups := map[string][]domain.Allocation{}
	seen := map[string]bool{}
	skus := map[string]bool{}
	for _, a := range r.Allocations {
		if seen[a.ID] || skus[a.SKU] {
			return domain.ErrInvalid
		}
		seen[a.ID] = true
		skus[a.SKU] = true
		wid := domain.Warehouse(a.WarehouseID)
		groups[wid] = append(groups[wid], domain.Allocation{ID: a.ID, SKU: a.SKU, Quantity: a.Quantity})
	}
	warehouses := make([]string, 0, len(groups))
	for wid := range groups {
		warehouses = append(warehouses, wid)
	}
	sort.Strings(warehouses)
	states := make([]domain.State, 0, len(groups))
	for _, wid := range warehouses {
		lines := groups[wid]
		sort.Slice(lines, func(i, j int) bool { return lines[i].SKU < lines[j].SKU })
		shipment, err := domain.Create(r.TenantID, domain.ShipmentID(r.ID, wid), r.OrderID, r.ID, wid, lines)
		if err != nil {
			return err
		}
		states = append(states, shipment.Snapshot())
	}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer cmdtx.Rollback(tx)
	tag, err := tx.Exec(ctx, `INSERT INTO fulfillment.inbox(tenant_id,id,order_id,request) VALUES($1,$2::uuid,$3::uuid,$4) ON CONFLICT DO NOTHING`, r.TenantID, r.ID, r.OrderID, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var same bool
		err = tx.QueryRow(ctx, `SELECT request=$3::jsonb FROM fulfillment.inbox WHERE tenant_id=$1 AND id=$2::uuid`, r.TenantID, r.ID, body).Scan(&same)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && !same {
			return domain.ErrConflict
		}
		return err
	}
	ctx, err = envelope.Worker(ctx, r.Metadata, "fulfillment", r.ID)
	if err != nil {
		return err
	}
	meta, _ := json.Marshal(r.Metadata)
	for _, state := range states {
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO fulfillment.shipments(tenant_id,id,order_id,checkout_id,warehouse_id,state,metadata) VALUES($1,$2::uuid,$3::uuid,$4::uuid,$5,$6,$7)`, state.TenantID, state.ID, state.OrderID, state.CheckoutID, state.WarehouseID, data, meta); err != nil {
			return err
		}
		for _, a := range state.Lines {
			if _, err = tx.Exec(ctx, `INSERT INTO fulfillment.allocations(tenant_id,id,shipment_id) VALUES($1,$2::uuid,$3::uuid)`, state.TenantID, a.ID, state.ID); err != nil {
				return err
			}
		}
		if err = event(ctx, tx, state, "requested"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Execute(ctx context.Context, r commands.Request, id string, expected int64, action string, fn func(*domain.Shipment) error) (domain.State, bool, error) {
	return cmdtx.Execute(ctx, s.Pool, "fulfillment", r, func(tx pgx.Tx) (domain.State, error) {
		var body []byte
		err := tx.QueryRow(ctx, `SELECT state FROM fulfillment.shipments WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, r.Tenant, id).Scan(&body)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.State{}, domain.ErrNotFound
		}
		if err != nil {
			return domain.State{}, err
		}
		shipment, err := restore(body)
		if err != nil {
			return domain.State{}, err
		}
		if shipment.Snapshot().Version != expected {
			return domain.State{}, domain.ErrConflict
		}
		if err = fn(shipment); err != nil {
			return domain.State{}, err
		}
		state := shipment.Snapshot()
		cause, err := identity.NewID()
		if err != nil {
			return domain.State{}, err
		}
		m, err := envelope.Record(ctx, "system:internal", cause)
		if err != nil {
			return domain.State{}, err
		}
		ctx = envelope.With(ctx, m)
		meta, _ := json.Marshal(m)
		body, err = json.Marshal(state)
		if err != nil {
			return domain.State{}, err
		}
		if _, err = tx.Exec(ctx, `UPDATE fulfillment.shipments SET state=$3,dispatch_metadata=CASE WHEN $4='dispatch' THEN $5::jsonb ELSE dispatch_metadata END,next_attempt_at=now() WHERE tenant_id=$1 AND id=$2::uuid`, r.Tenant, id, body, action, meta); err != nil {
			return domain.State{}, err
		}
		return state, event(ctx, tx, state, action)
	})
}
func scanView(row pgx.Row) (ports.View, error) {
	var v ports.View
	var body []byte
	err := row.Scan(&body, &v.Attempts, &v.LastError, &v.Metadata, &v.DispatchMetadata, &v.Paused, &v.ControlVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, domain.ErrNotFound
	}
	if err != nil {
		return v, err
	}
	shipment, err := restore(body)
	if err != nil {
		return v, err
	}
	v.State = shipment.Snapshot()
	return v, nil
}
func (s *Store) Get(ctx context.Context, tenant, id string) (ports.View, error) {
	return scanView(s.Pool.QueryRow(ctx, `SELECT state,attempts,coalesce(last_error,''),metadata,dispatch_metadata,paused,control_version FROM fulfillment.shipments WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id))
}
func (s *Store) ForOrder(ctx context.Context, tenant, id string) ([]ports.View, error) {
	rows, err := s.Pool.Query(ctx, `SELECT state,attempts,coalesce(last_error,''),metadata,dispatch_metadata,paused,control_version FROM fulfillment.shipments WHERE tenant_id=$1 AND order_id=$2::uuid ORDER BY warehouse_id,id`, tenant, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	views := []ports.View{}
	for rows.Next() {
		v, err := scanView(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, rows.Err()
}
func (s *Store) History(ctx context.Context, tenant, id string) ([]ports.Event, error) {
	if _, err := s.Get(ctx, tenant, id); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT version,action,state,metadata FROM fulfillment.events WHERE tenant_id=$1 AND shipment_id=$2::uuid ORDER BY version`, tenant, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []ports.Event{}
	for rows.Next() {
		var e ports.Event
		var body []byte
		if err = rows.Scan(&e.Version, &e.Action, &body, &e.Metadata); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &e.State); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) Claim(ctx context.Context) (ports.Job, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ports.Job{}, false, err
	}
	defer cmdtx.Rollback(tx)
	var body []byte
	var metadata envelope.Metadata
	err = tx.QueryRow(ctx, `SELECT state,CASE WHEN dispatch_metadata->>'version'='1' THEN dispatch_metadata ELSE metadata END FROM fulfillment.shipments WHERE NOT paused AND status='dispatching' AND next_attempt_at<=clock_timestamp() AND lease_until<=clock_timestamp() ORDER BY next_attempt_at,tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&body, &metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.Job{}, false, nil
	}
	if err != nil {
		return ports.Job{}, false, err
	}
	shipment, err := restore(body)
	if err != nil {
		return ports.Job{}, false, err
	}
	j := ports.Job{State: shipment.Snapshot(), Metadata: metadata}
	err = tx.QueryRow(ctx, `UPDATE fulfillment.shipments SET lease_token=lease_token+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE tenant_id=$1 AND id=$2::uuid RETURNING lease_token`, j.State.TenantID, j.State.ID).Scan(&j.Token)
	if err != nil {
		return ports.Job{}, false, err
	}
	return j, true, tx.Commit(ctx)
}
func (s *Store) Save(ctx context.Context, j ports.Job, retry bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer cmdtx.Rollback(tx)
	var body []byte
	err = tx.QueryRow(ctx, `SELECT state FROM fulfillment.shipments WHERE tenant_id=$1 AND id=$2::uuid AND lease_token=$3 FOR UPDATE`, j.State.TenantID, j.State.ID, j.Token).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLostLease
	}
	if err != nil {
		return err
	}
	shipment, err := restore(body)
	if err != nil {
		return err
	}
	if shipment.Snapshot().Version != j.State.Version {
		return ErrLostLease
	}
	delay, attempts := 0, 0
	var code any
	if retry {
		delay, attempts, code = 500, 1, "inventory_dispatch_retry"
	} else {
		if err = shipment.MarkDispatched(); err != nil {
			return err
		}
		body, err = json.Marshal(shipment.Snapshot())
		if err != nil {
			return err
		}
	}
	// Evaluate the lease after acquiring the row lock, at the final write.
	tag, err := tx.Exec(ctx, `UPDATE fulfillment.shipments SET state=$4,lease_until='-infinity',next_attempt_at=clock_timestamp()+($5*interval '1 millisecond'),attempts=attempts+$6,last_error=$7 WHERE tenant_id=$1 AND id=$2::uuid AND NOT paused AND lease_token=$3 AND lease_until>clock_timestamp()`, j.State.TenantID, j.State.ID, j.Token, body, delay, attempts, code)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLostLease
	}
	if !retry {
		if err = event(ctx, tx, shipment.Snapshot(), "allocation-dispatched"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
