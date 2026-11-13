package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const ServiceName = "atlas.inventory.v1.Inventory"

type Options struct {
	Store      ports.Store
	TenantKeys map[[32]byte]string
	Endpoint   string
	Logger     *slog.Logger
}
type server struct {
	pb.UnimplementedInventoryServer
	options Options
}

// New returns a unary API and standard health service. Call MonitorHealth while
// serving; the initial state is NOT_SERVING until a real readiness check passes.
func New(o Options) (*grpc.Server, *health.Server) {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	keys := make(map[[32]byte]string, len(o.TenantKeys))
	for k, v := range o.TenantKeys {
		keys[k] = v
	}
	o.TenantKeys = keys
	s := server{options: o}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10), grpc.MaxHeaderListSize(16<<10), grpc.MaxConcurrentStreams(64), grpc.ChainUnaryInterceptor(s.intercept))
	pb.RegisterInventoryServer(g, &s)
	h := health.NewServer()
	h.SetServingStatus("", hp.HealthCheckResponse_NOT_SERVING)
	h.SetServingStatus(ServiceName, hp.HealthCheckResponse_NOT_SERVING)
	hp.RegisterHealthServer(g, h)
	return g, h
}
func MonitorHealth(ctx context.Context, h *health.Server, ready func(context.Context) error) {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	defer h.Shutdown()
	for {
		check, cancel := context.WithTimeout(ctx, time.Second)
		state := hp.HealthCheckResponse_SERVING
		if ready == nil || ready(check) != nil {
			state = hp.HealthCheckResponse_NOT_SERVING
		}
		cancel()
		h.SetServingStatus("", state)
		h.SetServingStatus(ServiceName, state)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (s *server) intercept(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (reply any, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	defer func() {
		if recover() != nil {
			s.options.Logger.Error("inventory RPC panic")
			reply = nil
			err = status.Error(codes.Internal, "internal_error")
		}
	}()
	if info.FullMethod == hp.Health_Check_FullMethodName {
		return next(ctx, request)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, key := range []string{"x-actor-id", "x-causation-id", "x-request-id", "x-tenant-id"} {
		if len(md.Get(key)) != 0 {
			return nil, status.Error(codes.InvalidArgument, "server_owned_metadata")
		}
	}
	if len(md.Get("authorization")) != 1 {
		return nil, status.Error(codes.Unauthenticated, "unauthorized")
	}
	if len(md.Get("x-correlation-id")) > 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid_correlation_id")
	}
	correlation := ""
	if v := md.Get("x-correlation-id"); len(v) == 1 {
		correlation = v[0]
	}
	var e error
	ctx, m, e := auth.Authenticate(ctx, s.options.TenantKeys, md.Get("authorization")[0], correlation)
	if e != nil {
		if errors.Is(e, auth.ErrInvalidCorrelation) {
			return nil, status.Error(codes.InvalidArgument, "invalid_correlation_id")
		}
		if errors.Is(e, auth.ErrUnauthorized) {
			return nil, status.Error(codes.Unauthenticated, "unauthorized")
		}
		return nil, status.Error(codes.Unavailable, "identity_unavailable")
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", m.OriginRequestID, "x-correlation-id", m.CorrelationID, "x-atlas-endpoint", s.options.Endpoint))
	return next(ctx, request)
}
func (s *server) Reserve(ctx context.Context, r *pb.ReserveRequest) (*pb.ReservationReply, error) {
	if len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "unsupported_request_fields")
	}
	out, replayed, err := (reservestock.Handler{Store: s.options.Store}).Handle(ctx, auth.Tenant(ctx), r.IdempotencyKey, reservestock.Command{SKU: r.Sku, Quantity: r.Quantity, TTLSeconds: r.TtlSeconds, WarehouseID: r.WarehouseId})
	if err != nil {
		return nil, s.failure(err)
	}
	return &pb.ReservationReply{Reservation: reservation(*out.Reservation), Stock: &pb.Stock{TenantId: out.Stock.TenantID, Sku: out.Stock.SKU, OnHand: out.Stock.OnHand, Reserved: out.Stock.Reserved, Allocated: out.Stock.Allocated, Version: out.Stock.Version, WarehouseId: out.Stock.WarehouseID}, Replayed: replayed, ProtocolRevision: 2}, nil
}
func (s *server) GetReservation(ctx context.Context, r *pb.GetReservationRequest) (*pb.Reservation, error) {
	if len(r.ProtoReflect().GetUnknown()) != 0 || !identity.ValidID(r.Id) {
		return nil, status.Error(codes.InvalidArgument, "invalid_input")
	}
	out, err := s.options.Store.Reservation(ctx, auth.Tenant(ctx), r.Id)
	if err != nil {
		return nil, s.failure(err)
	}
	return reservation(out), nil
}
func reservation(r domain.ReservationState) *pb.Reservation {
	return &pb.Reservation{Id: r.ID, TenantId: r.TenantID, Sku: r.SKU, Quantity: r.Quantity, Status: r.Status, Version: r.Version, ExpiresAtUnixMs: r.ExpiresAt.UnixMilli(), WarehouseId: r.WarehouseID}
}
func (s *server) failure(err error) error {
	var sqlError interface{ SQLState() string }
	if errors.As(err, &sqlError) && (sqlError.SQLState() == "55P03" || sqlError.SQLState() == "57014") {
		return status.Error(codes.Unavailable, "database_budget_exceeded")
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request_canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline_exceeded")
	case errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid_input")
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "not_found")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency_conflict")
	case errors.Is(err, domain.ErrConflict):
		return status.Error(codes.Aborted, "concurrent_modification")
	case errors.Is(err, domain.ErrInsufficientStock):
		return status.Error(codes.FailedPrecondition, "insufficient_stock")
	default:
		s.options.Logger.Error("inventory RPC failed", "error", err)
		return status.Error(codes.Internal, "internal_error")
	}
}
