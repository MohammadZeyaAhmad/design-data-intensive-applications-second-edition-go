package routing

import (
	"context"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

type proxy struct {
	pb.UnimplementedInventoryServer
	pool *Pool
}

func NewProxy(p *Pool) (*grpc.Server, *health.Server) {
	g := grpc.NewServer(grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10), grpc.MaxHeaderListSize(16<<10), grpc.MaxConcurrentStreams(64))
	pb.RegisterInventoryServer(g, &proxy{pool: p})
	h := health.NewServer()
	h.SetServingStatus("", hp.HealthCheckResponse_NOT_SERVING)
	h.SetServingStatus(ServiceName, hp.HealthCheckResponse_NOT_SERVING)
	hp.RegisterHealthServer(g, h)
	return g, h
}
func (p *Pool) Monitor(ctx context.Context, h *health.Server) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	defer h.Shutdown()
	for {
		s := hp.HealthCheckResponse_NOT_SERVING
		if p.Ready() {
			s = hp.HealthCheckResponse_SERVING
		}
		h.SetServingStatus("", s)
		h.SetServingStatus(ServiceName, s)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
func forwarded(ctx context.Context) (context.Context, context.CancelFunc) {
	// WithTimeout retains an earlier incoming deadline. grpc-go transmits the
	// remaining budget, rather than starting a fresh budget at each hop.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	input, _ := metadata.FromIncomingContext(ctx)
	out := metadata.MD{}
	for _, key := range []string{"authorization", "x-correlation-id", "x-actor-id", "x-causation-id", "x-request-id", "x-tenant-id"} {
		if v := input.Get(key); len(v) > 0 {
			out[key] = append([]string(nil), v...)
		}
	}
	return metadata.NewOutgoingContext(ctx, out), cancel
}
func (p *proxy) Reserve(ctx context.Context, r *pb.ReserveRequest) (*pb.ReservationReply, error) {
	downstream, cancel := forwarded(ctx)
	defer cancel()
	var header metadata.MD
	out, e := p.pool.RPC.Reserve(downstream, r, grpc.Header(&header), grpc.WaitForReady(false))
	_ = grpc.SetHeader(ctx, header)
	return out, e
}
func (p *proxy) GetReservation(ctx context.Context, r *pb.GetReservationRequest) (*pb.Reservation, error) {
	downstream, cancel := forwarded(ctx)
	defer cancel()
	var header metadata.MD
	out, e := p.pool.RPC.GetReservation(downstream, r, grpc.Header(&header), grpc.WaitForReady(false))
	_ = grpc.SetHeader(ctx, header)
	return out, e
}
