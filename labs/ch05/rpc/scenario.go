// Package rpc exercises real Atlas and routing processes over HTTP and HTTP/2.
package rpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/rpc/wire/inventory/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Observation struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Observed any    `json:"observed"`
	Passed   bool   `json:"passed"`
}
type VersionSample struct {
	Client           string `json:"client"`
	Server           string `json:"server"`
	ReservationID    string `json:"reservation_id"`
	ProtocolRevision uint32 `json:"protocol_revision_seen"`
}
type Report struct {
	Scenario              string          `json:"scenario"`
	Builds                json.RawMessage `json:"binary_builds"`
	Versions              []VersionSample `json:"version_matrix"`
	RoutedEndpoints       []string        `json:"routed_endpoints"`
	Observations          []Observation   `json:"observations"`
	IncomingBudgetsMillis []int64         `json:"fault_proxy_remaining_budgets_ms"`
	CancelDrainNanos      int64           `json:"cancellation_database_drain_nanos"`
	DeadlineDrainNanos    int64           `json:"deadline_database_drain_nanos"`
	OrphanSQLDrainNanos   int64           `json:"killed_process_database_drain_nanos"`
	Verified              bool            `json:"verified"`
	Boundaries            []string        `json:"boundaries"`
}

func (r *Report) check(name, expected string, observed any, ok bool) error {
	r.Observations = append(r.Observations, Observation{Name: name, Expected: expected, Observed: observed, Passed: ok})
	if !ok {
		return fmt.Errorf("RPC observation failed: %s", name)
	}
	return nil
}
func writeRegistry(path string, revision int64, endpoints []string) error {
	b, e := json.Marshal(struct {
		Revision  int64    `json:"revision"`
		Endpoints []string `json:"endpoints"`
	}{revision, endpoints})
	if e != nil {
		return e
	}
	if e = os.WriteFile(path+".tmp", b, 0600); e != nil {
		return e
	}
	return os.Rename(path+".tmp", path)
}
func authContext(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}
func binaryManifest(dir string) (json.RawMessage, error) {
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return nil, e
	}
	if len(b) > 16384 {
		return nil, errors.New("binary manifest too large")
	}
	var m struct {
		Hashes map[string]string `json:"sha256"`
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	for _, name := range []string{"atlas", "router"} {
		f, e := os.Open(filepath.Join(dir, name))
		if e != nil {
			return nil, e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		f.Close()
		if e != nil {
			return nil, e
		}
		if hex.EncodeToString(h.Sum(nil)) != m.Hashes[name] {
			return nil, errors.New("binary digest mismatch")
		}
	}
	return b, nil
}
func Run(ctx context.Context, dsn, binaries, parent string) (report Report, runErr error) {
	stage := "preflight"
	defer func() {
		if runErr != nil {
			runErr = fmt.Errorf("RPC stage %s: %w", stage, runErr)
		}
	}()
	report.Scenario = "rpc-transport-failures"
	u, e := url.Parse(dsn)
	if e != nil || dsn == "" || u.Path != "/atlas_test" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "postgres") {
		return report, errors.New("bind ATLAS_TEST_DATABASE_URL to a local atlas_test database")
	}
	binaries, e = filepath.Abs(binaries)
	if e != nil {
		return report, e
	}
	report.Builds, e = binaryManifest(binaries)
	if e != nil {
		return report, e
	}
	if parent == "" {
		parent = os.TempDir()
	}
	if e = os.MkdirAll(parent, 0700); e != nil {
		return report, e
	}
	dir, e := os.MkdirTemp(parent, "rpc-run-")
	if e != nil {
		return report, e
	}
	dir, e = filepath.Abs(dir)
	if e != nil {
		return report, e
	}
	var nonce [24]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return report, e
	}
	token := hex.EncodeToString(nonce[:])
	database := "atlas_rpc_" + hex.EncodeToString(nonce[:8])
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return report, e
	}
	defer admin.Close()
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); e != nil {
		return report, errors.New("could not create isolated RPC database")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); e != nil && runErr == nil {
			runErr = errors.New("could not remove isolated RPC database")
		}
	}()
	u.Path = "/" + database
	isolatedDSN := u.String()
	db, e := pgxpool.New(ctx, isolatedDSN)
	if e != nil {
		return report, e
	}
	defer db.Close()
	env := map[string]string{"ATLAS_DATABASE_URL": isolatedDSN, "ATLAS_TENANT_KEYS": "rpc-lab=" + token, "ATLAS_PROVIDER_TOKEN": token, "ATLAS_GRAPHQL_CURSOR_KEY": token}
	atlas := filepath.Join(binaries, "atlas")
	migrate, e := startChild(ctx, atlas, dir, env, "migrate")
	if e != nil {
		return report, e
	}
	defer migrate.stop()
	select {
	case <-ctx.Done():
		return report, ctx.Err()
	case <-migrate.done:
		if migrate.err != nil {
			return report, errors.New("isolated RPC migration failed")
		}
	}
	stage = "REST startup"
	httpAddress, e := address()
	if e != nil {
		return report, e
	}
	env["ATLAS_HTTP_ADDR"] = httpAddress
	httpChild, e := startChild(ctx, atlas, dir, env, "serve")
	if e != nil {
		return report, e
	}
	defer httpChild.stop()
	base := "http://" + httpAddress
	if e = eventually(ctx, 15*time.Second, func() bool { _, _, e := rest(ctx, base, token, "GET", "/readyz", "", nil); return e == nil }); e != nil {
		return report, e
	}
	served, _, e := rest(ctx, base, token, "GET", "/openapi/inventory.json", "", nil)
	if e != nil {
		return report, e
	}
	if e = report.check("served OpenAPI matches the frozen client contract", "identical authored OpenAPI bytes", fmt.Sprintf("%x", sha256.Sum256(served)), bytes.Equal(served, openAPISpec)); e != nil {
		return report, e
	}
	seedResponse, _, e := rest(ctx, base, token, "POST", "/v1/stock/RPC/receipts", "seed", map[string]any{"quantity": 50})
	if e != nil {
		return report, e
	}
	if e = validateREST("ReceiveOutcome", seedResponse); e != nil {
		return report, e
	}
	leftAddress, e := address()
	if e != nil {
		return report, e
	}
	env["ATLAS_GRPC_ADDR"] = leftAddress
	env["ATLAS_RPC_ENDPOINT"] = "left"
	left, e := startChild(ctx, atlas, dir, env, "rpc")
	if e != nil {
		return report, e
	}
	defer left.stop()
	rightAddress, e := address()
	if e != nil {
		return report, e
	}
	env["ATLAS_GRPC_ADDR"] = rightAddress
	env["ATLAS_RPC_ENDPOINT"] = "right"
	right, e := startChild(ctx, atlas, dir, env, "rpc")
	if e != nil {
		return report, e
	}
	defer right.stop()
	stage = "backend health"
	if e = waitHealth(ctx, left, leftAddress); e != nil {
		return report, e
	}
	if e = waitHealth(ctx, right, rightAddress); e != nil {
		return report, e
	}
	leftConn, e := dial(leftAddress)
	if e != nil {
		return report, e
	}
	defer leftConn.Close()
	leftRPC := pb.NewInventoryClient(leftConn)
	legacyAddress, stopLegacy, e := serveLegacy(leftRPC)
	if e != nil {
		return report, e
	}
	defer stopLegacy()
	legacyConn, e := dial(legacyAddress)
	if e != nil {
		return report, e
	}
	defer legacyConn.Close()
	// Actual RPC calls use two independently decoded contract definitions in four
	// directions. No duplicate global protobuf descriptors are registered.
	stage = "version matrix"
	var reference *pb.ReservationReply
	for _, server := range []struct {
		name string
		conn *grpc.ClientConn
	}{{"current", leftConn}, {"baseline-descriptor-adapter", legacyConn}} {
		for _, version := range []string{"baseline", "current"} {
			key := version + "-to-" + server.name
			var reply *pb.ReservationReply
			if version == "baseline" {
				input, e := legacyRequest(key)
				if e != nil {
					return report, e
				}
				output, e := legacyMessage("ReservationReply")
				if e != nil {
					return report, e
				}
				if e = server.conn.Invoke(authContext(ctx, token), pb.Inventory_Reserve_FullMethodName, input, output); e != nil {
					return report, e
				}
				raw, e := proto.Marshal(output)
				if e != nil {
					return report, e
				}
				reply = &pb.ReservationReply{}
				if e = proto.Unmarshal(raw, reply); e != nil {
					return report, e
				}
				if server.name == "current" {
					reference = reply
					if e = report.check("old client preserves unknown binary reply fields", "revision 2 survives old descriptor relay", reply.ProtocolRevision, reply.ProtocolRevision == 2 && len(output.GetUnknown()) > 0); e != nil {
						return report, e
					}
					jsonReply, e := protojson.Marshal(output)
					if e != nil {
						return report, e
					}
					rebuilt := &pb.ReservationReply{}
					if e = protojson.Unmarshal(jsonReply, rebuilt); e != nil {
						return report, e
					}
					if e = report.check("JSON reconstruction loses unknown reply fields", "reconstructed protocol revision is absent", rebuilt.ProtocolRevision, rebuilt.ProtocolRevision == 0); e != nil {
						return report, e
					}
				}
			} else {
				reply, e = pb.NewInventoryClient(server.conn).Reserve(authContext(ctx, token), &pb.ReserveRequest{IdempotencyKey: key, Sku: "RPC", Quantity: 1})
				if e != nil {
					return report, e
				}
			}
			expectedRevision := uint32(2)
			if server.name != "current" {
				expectedRevision = 0
			}
			if reply.ProtocolRevision != expectedRevision {
				return report, errors.New("version response violated frozen descriptor shape")
			}
			if reply.Reservation == nil || reply.Reservation.Id == "" {
				return report, errors.New("version fixture lost common result fields")
			}
			report.Versions = append(report.Versions, VersionSample{Client: version, Server: server.name, ReservationID: reply.Reservation.Id, ProtocolRevision: reply.ProtocolRevision})
		}
	}
	_, e = pb.NewInventoryClient(legacyConn).Reserve(authContext(ctx, token), &pb.ReserveRequest{IdempotencyKey: "unsupported-warehouse", Sku: "RPC", Quantity: 1, WarehouseId: "north"})
	if e = report.check("new request semantics cannot disappear on an old adapter", "INVALID_ARGUMENT before command execution", status.Code(e).String(), status.Code(e) == codes.InvalidArgument); e != nil {
		return report, e
	}
	body, header, e := rest(ctx, base, token, "POST", "/v1/reservations", "baseline-to-current", map[string]any{"sku": "RPC", "quantity": 1, "ttl_seconds": nil, "warehouse_id": nil})
	if e != nil {
		return report, e
	}
	if e = validateREST("ReserveOutcome", body); e != nil {
		return report, fmt.Errorf("actual REST response violates OpenAPI: %w", e)
	}
	var restReply struct {
		Reservation struct {
			ID string `json:"id"`
		} `json:"reservation"`
	}
	if e = json.Unmarshal(body, &restReply); e != nil {
		return report, e
	}
	if e = report.check("REST and gRPC share command identity", "same reservation and saved replay", map[string]any{"id": restReply.Reservation.ID, "replayed": header.Get("Idempotent-Replay")}, restReply.Reservation.ID == reference.Reservation.Id && header.Get("Idempotent-Replay") == "true"); e != nil {
		return report, e
	}
	stockWire, _, e := rest(ctx, base, token, "GET", "/v1/stock/RPC", "", nil)
	if e != nil {
		return report, e
	}
	if e = validateREST("Stock", stockWire); e != nil {
		return report, e
	}
	reservationWire, _, e := rest(ctx, base, token, "GET", "/v1/reservations/"+reference.Reservation.Id, "", nil)
	if e != nil {
		return report, e
	}
	if e = validateREST("Reservation", reservationWire); e != nil {
		return report, e
	}
	if e = report.check("actual REST values satisfy OpenAPI schemas", "receipt, reserve, stock and reservation validated", 4, true); e != nil {
		return report, e
	}
	unreadyAddress, unready, stopUnready, e := serveUnready()
	if e != nil {
		return report, e
	}
	defer stopUnready()
	registry := filepath.Join(dir, "registry.json")
	if e = writeRegistry(registry, 1, []string{leftAddress, rightAddress, unreadyAddress}); e != nil {
		return report, e
	}
	routerAddress, e := address()
	if e != nil {
		return report, e
	}
	router, e := startChild(ctx, filepath.Join(binaries, "router"), dir, map[string]string{"ATLAS_RPC_REGISTRY": registry, "ATLAS_ROUTER_ADDR": routerAddress})
	if e != nil {
		return report, e
	}
	defer router.stop()
	stage = "router health"
	if e = waitHealth(ctx, router, routerAddress); e != nil {
		return report, e
	}
	routerConn, e := dial(routerAddress)
	if e != nil {
		return report, e
	}
	defer routerConn.Close()
	routed := pb.NewInventoryClient(routerConn)
	getEndpoint := func() string {
		attempt, cancel := context.WithTimeout(authContext(ctx, token), 300*time.Millisecond)
		defer cancel()
		var md metadata.MD
		_, e := routed.GetReservation(attempt, &pb.GetReservationRequest{Id: reference.Reservation.Id}, grpc.Header(&md))
		if e != nil {
			return ""
		}
		labels := md.Get("x-atlas-endpoint")
		if len(labels) != 1 {
			return ""
		}
		return labels[0]
	}
	stage = "round robin"
	seen := map[string]bool{}
	if e = eventually(ctx, 5*time.Second, func() bool {
		label := getEndpoint()
		if label != "" {
			seen[label] = true
		}
		return seen["left"] && seen["right"]
	}); e != nil {
		return report, e
	}
	report.RoutedEndpoints = []string{"left", "right"}
	if e = report.check("health-aware round robin across discovered processes", "both process labels; no unready calls", map[string]any{"seen": seen, "unready_calls": unready.calls.Load()}, len(seen) == 2 && unready.calls.Load() == 0); e != nil {
		return report, e
	}
	stage = "lost reply"
	fault := &faultProxy{backend: routed, committed: make(chan *pb.ReservationReply, 1)}
	fault.drop.Store(true)
	faultAddress, stopFault, e := serveFault(fault)
	if e != nil {
		return report, e
	}
	defer stopFault()
	faultConn, e := dial(faultAddress)
	if e != nil {
		return report, e
	}
	defer faultConn.Close()
	faultRPC := pb.NewInventoryClient(faultConn)
	lost := &pb.ReserveRequest{IdempotencyKey: "lost-reply", Sku: "RPC", Quantity: 2}
	short, cancel := context.WithTimeout(authContext(ctx, token), 500*time.Millisecond)
	_, callErr := faultRPC.Reserve(short, lost)
	cancel()
	var committed *pb.ReservationReply
	select {
	case committed = <-fault.committed:
	default:
		return report, errors.New("lost-reply fault did not observe a committed backend response")
	}
	if e = report.check("timed-out RPC already committed", "DEADLINE_EXCEEDED after confirmed durable backend reply", status.Code(callErr).String(), status.Code(callErr) == codes.DeadlineExceeded); e != nil {
		return report, e
	}
	retry, e := faultRPC.Reserve(authContext(ctx, token), lost)
	if e != nil {
		return report, e
	}
	var receipts, reservations, outbox int
	if e = db.QueryRow(ctx, `SELECT count(*) FROM inventory.command_results WHERE tenant_id='rpc-lab' AND operation='reserve-stock' AND idempotency_key='lost-reply'`).Scan(&receipts); e != nil {
		return report, e
	}
	if e = db.QueryRow(ctx, `SELECT count(*) FROM inventory.reservations WHERE tenant_id='rpc-lab' AND id=$1`, committed.Reservation.Id).Scan(&reservations); e != nil {
		return report, e
	}
	if e = db.QueryRow(ctx, `SELECT count(*) FROM inventory.outbox WHERE tenant_id='rpc-lab' AND payload->'reservation'->>'id'=$1`, committed.Reservation.Id).Scan(&outbox); e != nil {
		return report, e
	}
	if e = report.check("same-key retry returns one committed reservation", "1 receipt, 1 reservation, 1 outbox event", map[string]any{"receipts": receipts, "reservations": reservations, "outbox": outbox, "replayed": retry.Replayed}, receipts == 1 && reservations == 1 && outbox == 1 && retry.Replayed && retry.Reservation.Id == committed.Reservation.Id); e != nil {
		return report, e
	}
	changed := proto.Clone(lost).(*pb.ReserveRequest)
	changed.Quantity++
	_, callErr = routed.Reserve(authContext(ctx, token), changed)
	if e = report.check("changed payload cannot reuse a receipt", "ALREADY_EXISTS", status.Code(callErr).String(), status.Code(callErr) == codes.AlreadyExists); e != nil {
		return report, e
	}
	// Counterexample: routing and transport do not know that two command IDs
	// represent one user intention. Distinct keys legitimately create two holds.
	a, e := routed.Reserve(authContext(ctx, token), &pb.ReserveRequest{IdempotencyKey: "new-key-counterexample-a", Sku: "RPC", Quantity: 1})
	if e != nil {
		return report, e
	}
	b, e := routed.Reserve(authContext(ctx, token), &pb.ReserveRequest{IdempotencyKey: "new-key-counterexample-b", Sku: "RPC", Quantity: 1})
	if e != nil {
		return report, e
	}
	if e = report.check("new-key retry counterexample", "different keys create different reservations", []string{a.Reservation.Id, b.Reservation.Id}, a.Reservation.Id != b.Reservation.Id); e != nil {
		return report, e
	}
	// Withdraw right, then kill left only after PostgreSQL confirms its in-flight
	// command is waiting on our lock. The transaction cannot have reserved stock.
	stage = "withdraw right"
	if e = writeRegistry(registry, 2, []string{leftAddress}); e != nil {
		return report, e
	}
	consecutive := 0
	if e = eventually(ctx, 5*time.Second, func() bool {
		if getEndpoint() == "left" {
			consecutive++
		} else {
			consecutive = 0
		}
		return consecutive >= 6
	}); e != nil {
		return report, e
	}
	held, e := db.Begin(ctx)
	if e != nil {
		return report, e
	}
	defer held.Rollback(context.Background())
	if _, e = held.Exec(ctx, `SELECT 1 FROM inventory.stock WHERE tenant_id='rpc-lab' AND sku='RPC' FOR UPDATE`); e != nil {
		return report, e
	}
	inflight, cancelInflight := context.WithTimeout(authContext(ctx, token), 3*time.Second)
	defer cancelInflight()
	gone := &pb.ReserveRequest{IdempotencyKey: "endpoint-disappeared", Sku: "RPC", Quantity: 1}
	done := make(chan error, 1)
	go func() { _, e := routed.Reserve(inflight, gone); done <- e }()
	stage = "kill blocked left"
	if e = waitDatabaseLock(ctx, db, "left", true); e != nil {
		return report, e
	}
	orphanStart := time.Now()
	left.kill()
	callErr = <-done
	cancelInflight()
	if e = report.check("endpoint disappears during an actual locked command", "UNAVAILABLE after real backend SIGKILL", status.Code(callErr).String(), status.Code(callErr) == codes.Unavailable); e != nil {
		return report, e
	}
	if e = eventually(ctx, 2*time.Second, func() bool {
		var n int
		e := db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-rpc/left'`).Scan(&n)
		return e == nil && n == 0
	}); e != nil {
		return report, fmt.Errorf("killed backend sessions did not drain: %w", e)
	}
	report.OrphanSQLDrainNanos = time.Since(orphanStart).Nanoseconds()
	var killedReceipts int
	if e = db.QueryRow(ctx, `SELECT count(*) FROM inventory.command_results WHERE tenant_id='rpc-lab' AND operation='reserve-stock' AND idempotency_key='endpoint-disappeared'`).Scan(&killedReceipts); e != nil {
		return report, e
	}
	if e = report.check("orphan SQL is bounded after process death", "all killed sessions drained and 0 receipts before releasing the stock lock", map[string]any{"drain_nanos": report.OrphanSQLDrainNanos, "receipts": killedReceipts}, killedReceipts == 0); e != nil {
		return report, e
	}
	if e = held.Rollback(ctx); e != nil {
		return report, e
	}
	stage = "discover right"
	if e = writeRegistry(registry, 3, []string{rightAddress}); e != nil {
		return report, e
	}
	if e = eventually(ctx, 5*time.Second, func() bool { return getEndpoint() == "right" }); e != nil {
		return report, e
	}
	recovered, e := routed.Reserve(authContext(ctx, token), gone)
	if e != nil {
		return report, e
	}
	if e = report.check("discovery update recovers same command identity", "new endpoint executes once after old transaction rollback", map[string]any{"endpoint": "right", "replayed": recovered.Replayed}, !recovered.Replayed && recovered.Reservation.Id != ""); e != nil {
		return report, e
	}
	stage = "cancellation"
	if e = checkCancellation(ctx, db, token, faultRPC, &report); e != nil {
		return report, e
	}
	stage = "full withdrawal"
	if e = writeRegistry(registry, 4, []string{}); e != nil {
		return report, e
	}
	h := hp.NewHealthClient(routerConn)
	if e = eventually(ctx, 5*time.Second, func() bool {
		attempt, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		r, e := h.Check(attempt, &hp.HealthCheckRequest{Service: "atlas.inventory.v1.Inventory"})
		return e == nil && r.Status == hp.HealthCheckResponse_NOT_SERVING
	}); e != nil {
		return report, e
	}
	_, callErr = routed.GetReservation(authContext(ctx, token), &pb.GetReservationRequest{Id: reference.Reservation.Id})
	if e = report.check("empty discovery snapshot withdraws every endpoint", "UNAVAILABLE and router NOT_SERVING", status.Code(callErr).String(), status.Code(callErr) == codes.Unavailable); e != nil {
		return report, e
	}
	if e = writeRegistry(registry, 5, []string{rightAddress}); e != nil {
		return report, e
	}
	if e = eventually(ctx, 5*time.Second, func() bool { return getEndpoint() == "right" }); e != nil {
		return report, e
	}
	if e = report.check("routing recovers after complete withdrawal", "healthy right process selected", getEndpoint(), getEndpoint() == "right" && unready.calls.Load() == 0); e != nil {
		return report, e
	}
	fault.mu.Lock()
	report.IncomingBudgetsMillis = append([]int64(nil), fault.budgets...)
	fault.mu.Unlock()
	report.Verified = true
	report.Boundaries = []string{"Loopback plaintext and development bearer keys; production mesh identity/TLS and registry consensus are separate work.", "Registry revisions are supplied by one trusted operator file; health is reachability/readiness, not business correctness.", "Baseline server is a frozen-descriptor adapter over the real backend, not a historical released Atlas binary.", "A timeout cannot prove rollback. Receipt lifetime and any external side effects require their own retention and consistency policy.", "Proxy performs one forwarding attempt. Caller retries must retain command identity and the original total deadline."}
	return report, nil
}
func waitDatabaseLock(ctx context.Context, db *pgxpool.Pool, endpoint string, want bool) error {
	e := eventually(ctx, 2*time.Second, func() bool {
		var n int
		e := db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND wait_event_type='Lock'`, "atlas-rpc/"+endpoint).Scan(&n)
		return e == nil && (n > 0) == want
	})
	if e != nil {
		rows, qe := db.Query(ctx, `SELECT application_name,coalesce(wait_event_type,''),coalesce(wait_event,''),state FROM pg_stat_activity WHERE datname=current_database()`)
		states := [][]string{}
		if qe == nil {
			defer rows.Close()
			for rows.Next() {
				var app, kind, event, state string
				if rows.Scan(&app, &kind, &event, &state) == nil {
					states = append(states, []string{app, kind, event, state})
				}
			}
		}
		return fmt.Errorf("database lock endpoint=%s want=%t: %w; states=%v", endpoint, want, e, states)
	}
	return nil
}
func checkCancellation(ctx context.Context, db *pgxpool.Pool, token string, rpc pb.InventoryClient, report *Report) error {
	held, e := db.Begin(ctx)
	if e != nil {
		return e
	}
	defer held.Rollback(context.Background())
	if _, e = held.Exec(ctx, `SELECT 1 FROM inventory.stock WHERE tenant_id='rpc-lab' AND sku='RPC' FOR UPDATE`); e != nil {
		return e
	}
	for _, explicit := range []bool{true, false} {
		budget := 250 * time.Millisecond
		if explicit {
			budget = 2 * time.Second
		}
		attempt, cancel := context.WithTimeout(authContext(ctx, token), budget)
		done := make(chan error, 1)
		key := "expired-budget"
		if explicit {
			key = "explicit-cancel"
		}
		go func() {
			_, e := rpc.Reserve(attempt, &pb.ReserveRequest{IdempotencyKey: key, Sku: "RPC", Quantity: 1})
			done <- e
		}()
		if e = waitDatabaseLock(ctx, db, "right", true); e != nil {
			cancel()
			return e
		}
		start := time.Now()
		if explicit {
			cancel()
		}
		callErr := <-done
		cancel()
		if e = waitDatabaseLock(ctx, db, "right", false); e != nil {
			return e
		}
		elapsed := time.Since(start).Nanoseconds()
		expected := codes.DeadlineExceeded
		name := "deadline cancels database lock wait across proxies"
		if explicit {
			expected = codes.Canceled
			name = "explicit cancellation reaches the database"
			report.CancelDrainNanos = elapsed
		} else {
			report.DeadlineDrainNanos = elapsed
		}
		var receipts int
		if e = db.QueryRow(ctx, `SELECT count(*) FROM inventory.command_results WHERE tenant_id='rpc-lab' AND operation='reserve-stock' AND idempotency_key=$1`, key).Scan(&receipts); e != nil {
			return e
		}
		if e = report.check(name, "expected transport status; 0 receipts while original row lock remains held", map[string]any{"status": status.Code(callErr).String(), "receipts": receipts, "drain_nanos": elapsed}, status.Code(callErr) == expected && receipts == 0); e != nil {
			return e
		}
	}
	return held.Rollback(ctx)
}
