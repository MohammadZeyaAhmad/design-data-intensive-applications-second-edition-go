//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	client "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/client"
	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/grpcapi"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/httpapi"
	invdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/database"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/routing"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

const rpcToken = "rpc-fixture-key-long-enough-for-local-tests"

func rpcContext(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+rpcToken))
}
func listenRPC(t *testing.T, g *grpc.Server) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)
	return l.Addr().String()
}
func rpcDial(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	c, e := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func rpcBackend(t *testing.T, id string, s ports.Store, label string) (string, *grpc.Server) {
	t.Helper()
	g, h := grpcapi.New(grpcapi.Options{Store: s, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}, Endpoint: label})
	h.SetServingStatus(grpcapi.ServiceName, hp.HealthCheckResponse_SERVING)
	h.SetServingStatus("", hp.HealthCheckResponse_SERVING)
	return listenRPC(t, g), g
}
func TestRPCRESTParityIdentityAndTenantIsolation(t *testing.T) {
	id := tenant(t)
	other := tenant(t)
	receive(t, id, "RPC", "seed", 20)
	keys := map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id, sha256.Sum256([]byte(rpcToken + "-other")): other}
	g, _ := grpcapi.New(grpcapi.Options{Store: store, TenantKeys: keys})
	address := listenRPC(t, g)
	rpc := pb.NewInventoryClient(rpcDial(t, address))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = rpcContext(ctx)
	out, e := rpc.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "same-command", Sku: "RPC", Quantity: 2, WarehouseId: "default"})
	if e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/reservations", bytes.NewBufferString(`{"sku":"RPC","quantity":2}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rpcToken)
	req.Header.Set("Idempotency-Key", "same-command")
	httpapi.New(httpapi.Options{Store: store, TenantKeys: keys, Ready: func(context.Context) error { return nil }}).ServeHTTP(w, req)
	var rest ports.Outcome
	if e = json.Unmarshal(w.Body.Bytes(), &rest); e != nil {
		t.Fatal(e)
	}
	if w.Code != 201 || w.Header().Get("Idempotent-Replay") != "true" || rest.Reservation == nil || rest.Reservation.ID != out.Reservation.Id || rest.Stock.Version != out.Stock.Version {
		t.Fatalf("REST/RPC mismatch: %d %s", w.Code, w.Body.String())
	}
	if count(t, "reservations", id) != 1 || count(t, "outbox", id) != 2 || count(t, "command_results", id) != 2 {
		t.Fatal("transport retry duplicated business effects")
	}
	_, e = rpc.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "same-command", Sku: "RPC", Quantity: 3})
	if status.Code(e) != codes.AlreadyExists {
		t.Fatal("identity conflict:", e)
	}
	_, e = rpc.GetReservation(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+rpcToken+"-other")), &pb.GetReservationRequest{Id: out.Reservation.Id})
	if status.Code(e) != codes.NotFound {
		t.Fatal("tenant escaped:", e)
	}
	for _, md := range []metadata.MD{metadata.Pairs("authorization", "Bearer "+rpcToken, "x-tenant-id", other), metadata.Pairs("authorization", "Bearer "+rpcToken, "authorization", "Bearer "+rpcToken), metadata.Pairs("authorization", "Bearer "+rpcToken, "x-actor-id", "operator:local")} {
		_, e = rpc.GetReservation(metadata.NewOutgoingContext(ctx, md), &pb.GetReservationRequest{Id: out.Reservation.Id})
		if status.Code(e) != codes.InvalidArgument && status.Code(e) != codes.Unauthenticated {
			t.Fatal("ambiguous identity accepted:", e)
		}
	}
	unknown := &pb.ReserveRequest{IdempotencyKey: "future-command", Sku: "RPC", Quantity: 1}
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	_, e = rpc.Reserve(ctx, unknown)
	if status.Code(e) != codes.InvalidArgument || count(t, "reservations", id) != 1 {
		t.Fatal("future semantics silently ignored:", e)
	}
	_, e = rpc.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "too-much", Sku: "RPC", Quantity: 100})
	if status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	_, e = rpc.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "oversized", Sku: strings.Repeat("x", 65<<10), Quantity: 1})
	if status.Code(e) != codes.ResourceExhausted || count(t, "reservations", id) != 1 {
		t.Fatal("oversized RPC reached business work:", e)
	}
}

func TestRPCRESTDeadlineCancelsPostgres(t *testing.T) {
	id := tenant(t)
	receive(t, id, "HTTP-BUDGET", "seed", 10)
	cfg := pool.Config().Copy()
	database.ConfigureHTTP(cfg)
	apiPool, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer apiPool.Close()
	keys := map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}
	api := httptest.NewServer(httpapi.New(httpapi.Options{Store: &invdb.Store{Pool: apiPool}, TenantKeys: keys, Ready: func(context.Context) error { return nil }}))
	defer api.Close()
	held, e := pool.Begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer held.Rollback(context.Background())
	if _, e = held.Exec(context.Background(), `SELECT 1 FROM inventory.stock WHERE tenant_id=$1 AND sku='HTTP-BUDGET' FOR UPDATE`, id); e != nil {
		t.Fatal(e)
	}
	makeRequest := func() (*http.Response, error) {
		req, e := http.NewRequest("POST", api.URL+"/v1/reservations", strings.NewReader(`{"sku":"HTTP-BUDGET","quantity":1}`))
		if e != nil {
			return nil, e
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+rpcToken)
		req.Header.Set("Idempotency-Key", "http-budget")
		req.Header.Set("X-Request-Timeout-Ms", "300")
		return (&http.Client{Timeout: 2 * time.Second}).Do(req)
	}
	done := make(chan error, 1)
	responseCode := make(chan int, 1)
	go func() {
		r, e := makeRequest()
		if e == nil {
			responseCode <- r.StatusCode
			r.Body.Close()
		}
		done <- e
	}()
	waitRPC(t, func() bool {
		var n int
		e := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-http/local' AND wait_event_type='Lock'`).Scan(&n)
		return e == nil && n > 0
	})
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if code := <-responseCode; code != 504 {
		t.Fatalf("REST timeout became status %d", code)
	}
	waitRPC(t, func() bool {
		var n int
		e := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-http/local' AND wait_event_type='Lock'`).Scan(&n)
		return e == nil && n == 0
	})
	if count(t, "reservations", id) != 0 || count(t, "command_results", id) != 1 || count(t, "outbox", id) != 1 {
		t.Fatal("HTTP deadline committed partial work")
	}
	if e = held.Rollback(context.Background()); e != nil {
		t.Fatal(e)
	}
	response, e := makeRequest()
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.StatusCode != 201 || response.Header.Get("Idempotent-Replay") != "false" {
		t.Fatal("HTTP command identity did not recover after cancellation")
	}
}

type observedRPCStore struct {
	ports.Store
	started   chan time.Time
	finished  chan error
	dropFirst atomic.Bool
}

func (s *observedRPCStore) Execute(ctx context.Context, r ports.Request, fn func(context.Context, ports.Transaction) (ports.Outcome, error)) (ports.Outcome, bool, error) {
	if s.started != nil {
		deadline, _ := ctx.Deadline()
		s.started <- deadline
	}
	out, replay, e := s.Store.Execute(ctx, r, fn)
	if e == nil && s.dropFirst.CompareAndSwap(true, false) {
		<-ctx.Done()
		e = ctx.Err()
	}
	if s.finished != nil {
		s.finished <- ctx.Err()
	}
	return out, replay, e
}
func TestRPCLostReplyRetriesOneDurableResult(t *testing.T) {
	id := tenant(t)
	receive(t, id, "LOST", "seed", 10)
	s := &observedRPCStore{Store: store}
	s.dropFirst.Store(true)
	address, _ := rpcBackend(t, id, s, "lost")
	c := client.Client{RPC: pb.NewInventoryClient(rpcDial(t, address)), Token: rpcToken}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, e := c.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "lost-response", Sku: "LOST", Quantity: 2})
	if e != nil {
		t.Fatal(e)
	}
	if !out.Replayed || out.Stock.Reserved != 2 || count(t, "reservations", id) != 1 || count(t, "outbox", id) != 2 || count(t, "command_results", id) != 2 {
		t.Fatalf("uncertain commit retry duplicated: %+v", out)
	}
	var actor string
	if e = pool.QueryRow(ctx, `SELECT metadata->>'actor' FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.stock-reserved.v1'`, id).Scan(&actor); e != nil {
		t.Fatal(e)
	}
	if actor != "tenant-key:"+id {
		t.Fatal("RPC command lost authenticated lineage", actor)
	}
}
func TestRPCParentBudgetAndCancellationRollbackAcrossProxy(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BUDGET", "seed", 10)
	cfg := pool.Config().Copy()
	if e := database.ConfigureRPC(cfg, "test-budget"); e != nil {
		t.Fatal(e)
	}
	rpcPool, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer rpcPool.Close()
	s := &observedRPCStore{Store: &invdb.Store{Pool: rpcPool}, started: make(chan time.Time, 2), finished: make(chan error, 2)}
	address, _ := rpcBackend(t, id, s, "budget")
	p, e := routing.NewLocal(routing.Registry{Revision: 1, Endpoints: []string{address}})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	g, _ := routing.NewProxy(p)
	rpc := pb.NewInventoryClient(rpcDial(t, listenRPC(t, g)))
	waitRPC(t, func() bool { return p.Ready() })
	tx, e := pool.Begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(context.Background(), `SELECT 1 FROM inventory.stock WHERE tenant_id=$1 AND sku='BUDGET' FOR UPDATE`, id); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(rpcContext(context.Background()), 250*time.Millisecond)
		parent, _ := ctx.Deadline()
		done := make(chan error, 1)
		go func() {
			_, e := rpc.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "blocked", Sku: "BUDGET", Quantity: 1})
			done <- e
		}()
		select {
		case deadline := <-s.started:
			if deadline.After(parent.Add(5 * time.Millisecond)) {
				t.Fatal("proxy renewed deadline")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request never reached backend")
		}
		if i == 0 {
			cancel()
		}
		e = <-done
		cancel()
		expected := codes.Canceled
		if i == 1 {
			expected = codes.DeadlineExceeded
		}
		if status.Code(e) != expected {
			t.Fatal(e)
		}
		select {
		case e = <-s.finished:
			if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("backend context survived:", e)
			}
		case <-time.After(time.Second):
			t.Fatal("backend lock wait survived caller cancellation")
		}
	}
	waitRPC(t, func() bool {
		var n int
		e := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-rpc/test-budget' AND wait_event_type='Lock'`).Scan(&n)
		return e == nil && n == 0
	})
	if count(t, "reservations", id) != 0 || count(t, "command_results", id) != 1 || count(t, "outbox", id) != 1 {
		t.Fatal("pre-commit cancellation left partial effects")
	}
	if e = tx.Rollback(context.Background()); e != nil {
		t.Fatal(e)
	}
	c := client.Client{RPC: rpc, Token: rpcToken}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, e = c.Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "blocked", Sku: "BUDGET", Quantity: 1}); e != nil {
		t.Fatal("same identity could not recover:", e)
	}
}
func waitRPC(t *testing.T, fn func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("RPC condition did not converge")
}
func TestRPCHealthDiscoveryAndFullWithdrawal(t *testing.T) {
	id := tenant(t)
	receive(t, id, "ROUTE", "seed", 10)
	direct, _ := rpcBackend(t, id, store, "seed")
	ctx, cancel := context.WithTimeout(rpcContext(context.Background()), 10*time.Second)
	defer cancel()
	created, e := pb.NewInventoryClient(rpcDial(t, direct)).Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "route", Sku: "ROUTE", Quantity: 1})
	if e != nil {
		t.Fatal(e)
	}
	endpoints := []string{}
	servers := []*grpc.Server{}
	healths := []interface {
		SetServingStatus(string, hp.HealthCheckResponse_ServingStatus)
	}{}
	for _, name := range []string{"a", "b", "unready"} {
		g, h := grpcapi.New(grpcapi.Options{Store: store, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}, Endpoint: name})
		if name != "unready" {
			h.SetServingStatus(grpcapi.ServiceName, hp.HealthCheckResponse_SERVING)
		}
		endpoints = append(endpoints, listenRPC(t, g))
		servers = append(servers, g)
		healths = append(healths, h)
	}
	p, e := routing.NewLocal(routing.Registry{Revision: 1, Endpoints: endpoints})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	g, _ := routing.NewProxy(p)
	rpc := pb.NewInventoryClient(rpcDial(t, listenRPC(t, g)))
	seen := map[string]bool{}
	get := func() string {
		var header metadata.MD
		_, e := rpc.GetReservation(ctx, &pb.GetReservationRequest{Id: created.Reservation.Id}, grpc.Header(&header))
		if e != nil {
			return ""
		}
		labels := header.Get("x-atlas-endpoint")
		if len(labels) != 1 {
			t.Fatal("endpoint evidence missing")
		}
		if labels[0] == "unready" {
			t.Fatal("NOT_SERVING endpoint received business RPC")
		}
		return labels[0]
	}
	waitRPC(t, func() bool { seen[get()] = true; return seen["a"] && seen["b"] })
	healths[1].SetServingStatus(grpcapi.ServiceName, hp.HealthCheckResponse_NOT_SERVING)
	consecutive := 0
	waitRPC(t, func() bool {
		if get() == "a" {
			consecutive++
		} else {
			consecutive = 0
		}
		return consecutive >= 6
	})
	if e = p.Apply(routing.Registry{Revision: 2, Endpoints: []string{}}); e != nil {
		t.Fatal(e)
	}
	waitRPC(t, func() bool { return !p.Ready() })
	_, e = rpc.GetReservation(ctx, &pb.GetReservationRequest{Id: created.Reservation.Id})
	if status.Code(e) != codes.Unavailable {
		t.Fatal("full withdrawal retained stale destination:", e)
	}
	if e = p.Apply(routing.Registry{Revision: 3, Endpoints: []string{endpoints[0]}}); e != nil {
		t.Fatal(e)
	}
	waitRPC(t, func() bool { return get() == "a" })
	servers[0].Stop()
	waitRPC(t, func() bool { return !p.Ready() })
}
