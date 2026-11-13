package rpc

import (
	"context"
	_ "embed"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/rpc/wire/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"net"
)

//go:embed wire/legacy.pb
var legacyDescriptor []byte

func outgoing(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	out := metadata.MD{}
	for _, key := range []string{"authorization", "x-correlation-id", "x-actor-id", "x-causation-id", "x-request-id", "x-tenant-id"} {
		if v := md.Get(key); len(v) > 0 {
			out[key] = append([]string(nil), v...)
		}
	}
	return metadata.NewOutgoingContext(ctx, out)
}

type faultProxy struct {
	pb.UnimplementedInventoryServer
	backend   pb.InventoryClient
	drop      atomic.Bool
	committed chan *pb.ReservationReply
	mu        sync.Mutex
	budgets   []int64
}

func (p *faultProxy) Reserve(ctx context.Context, r *pb.ReserveRequest) (*pb.ReservationReply, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	p.mu.Lock()
	p.budgets = append(p.budgets, time.Until(deadline).Milliseconds())
	p.mu.Unlock()
	out, e := p.backend.Reserve(outgoing(ctx), r)
	if e == nil && p.drop.CompareAndSwap(true, false) {
		p.committed <- proto.Clone(out).(*pb.ReservationReply)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return out, e
}
func (p *faultProxy) GetReservation(ctx context.Context, r *pb.GetReservationRequest) (*pb.Reservation, error) {
	return p.backend.GetReservation(outgoing(ctx), r)
}
func serveFault(p *faultProxy) (string, func(), error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", nil, e
	}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10))
	pb.RegisterInventoryServer(g, p)
	go func() { _ = g.Serve(l) }()
	return l.Addr().String(), g.Stop, nil
}

type unready struct {
	pb.UnimplementedInventoryServer
	calls atomic.Int64
}

func (u *unready) GetReservation(context.Context, *pb.GetReservationRequest) (*pb.Reservation, error) {
	u.calls.Add(1)
	return nil, status.Error(codes.Internal, "unhealthy_endpoint_received_business_call")
}
func serveUnready() (string, *unready, func(), error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", nil, nil, e
	}
	u := &unready{}
	g := grpc.NewServer()
	pb.RegisterInventoryServer(g, u)
	h := health.NewServer()
	h.SetServingStatus("atlas.inventory.v1.Inventory", hp.HealthCheckResponse_NOT_SERVING)
	hp.RegisterHealthServer(g, h)
	go func() { _ = g.Serve(l) }()
	return l.Addr().String(), u, g.Stop, nil
}
func legacyMessage(name string) (*dynamicpb.Message, error) {
	var set descriptorpb.FileDescriptorSet
	if e := proto.Unmarshal(legacyDescriptor, &set); e != nil {
		return nil, e
	}
	files, e := protodesc.NewFiles(&set)
	if e != nil {
		return nil, e
	}
	d, e := files.FindDescriptorByName(protoreflect.FullName("atlas.inventory.v1." + name))
	if e != nil {
		return nil, e
	}
	m, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, errors.New("legacy descriptor is not a message")
	}
	return dynamicpb.NewMessage(m), nil
}
func legacyRequest(key string) (*dynamicpb.Message, error) {
	m, e := legacyMessage("ReserveRequest")
	if e != nil {
		return nil, e
	}
	fields := m.Descriptor().Fields()
	m.Set(fields.ByNumber(1), protoreflect.ValueOfString(key))
	m.Set(fields.ByNumber(2), protoreflect.ValueOfString("RPC"))
	m.Set(fields.ByNumber(3), protoreflect.ValueOfInt64(1))
	return m, nil
}

// This lab-only baseline adapter decodes the frozen descriptor privately. It
// delegates business work to the real backend, and returns only original fields.
// It is a version fixture, not a claim about a historical Atlas deployment.
func serveLegacy(backend pb.InventoryClient) (string, func(), error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", nil, e
	}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10))
	desc := grpc.ServiceDesc{ServiceName: "atlas.inventory.v1.Inventory", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "Reserve", Handler: func(_ any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		in, e := legacyMessage("ReserveRequest")
		if e != nil {
			return nil, e
		}
		if e = decode(in); e != nil {
			return nil, e
		}
		if len(in.GetUnknown()) != 0 {
			return nil, status.Error(codes.InvalidArgument, "unsupported_request_fields")
		}
		raw, e := proto.Marshal(in)
		if e != nil {
			return nil, e
		}
		current := &pb.ReserveRequest{}
		if e = proto.Unmarshal(raw, current); e != nil {
			return nil, e
		}
		reply, e := backend.Reserve(outgoing(ctx), current)
		if e != nil {
			return nil, e
		}
		raw, e = proto.Marshal(reply)
		if e != nil {
			return nil, e
		}
		old, e := legacyMessage("ReservationReply")
		if e != nil {
			return nil, e
		}
		if e = (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, old); e != nil {
			return nil, e
		}
		return old, nil
	}}}}
	g.RegisterService(&desc, struct{}{})
	go func() { _ = g.Serve(l) }()
	return l.Addr().String(), g.Stop, nil
}
