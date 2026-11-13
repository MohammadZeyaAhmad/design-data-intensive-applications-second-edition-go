// Package client provides bounded retries for the published Inventory service.
// It deliberately has no generic "retry any RPC" method.
package client

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	RPC   pb.InventoryClient
	Token string
	conn  *grpc.ClientConn
}

// New uses gRPC's target resolver (for example dns:///inventory:8083), standard
// health Watch, round robin and explicit transport credentials. Service-config
// retries are disabled so retry ownership stays with this bounded caller.
func New(target, token string, transport credentials.TransportCredentials) (Client, error) {
	if target == "" || transport == nil || len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, " \t\r\n") {
		return Client{}, errors.New("target, transport credentials and tenant key are required")
	}
	conn, e := grpc.NewClient(target, grpc.WithTransportCredentials(transport), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}],"healthCheckConfig":{"serviceName":"atlas.inventory.v1.Inventory"}}`))
	if e != nil {
		return Client{}, e
	}
	return Client{RPC: pb.NewInventoryClient(conn), Token: token, conn: conn}, nil
}
func (c Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Reserve snapshots the request once. Every attempt uses exactly that payload
// and key; a new key after an uncertain response is a new business command.
func (c Client) Reserve(ctx context.Context, r *pb.ReserveRequest) (*pb.ReservationReply, error) {
	if r == nil || !validKey(r.IdempotencyKey) {
		return nil, status.Error(codes.InvalidArgument, "stable_idempotency_key_required")
	}
	snapshot := proto.Clone(r).(*pb.ReserveRequest)
	return retry(ctx, c.Token, func(attempt context.Context) (*pb.ReservationReply, error) {
		return c.RPC.Reserve(attempt, snapshot, grpc.WaitForReady(false))
	})
}
func (c Client) GetReservation(ctx context.Context, r *pb.GetReservationRequest) (*pb.Reservation, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "request_required")
	}
	snapshot := proto.Clone(r).(*pb.GetReservationRequest)
	return retry(ctx, c.Token, func(attempt context.Context) (*pb.Reservation, error) {
		return c.RPC.GetReservation(attempt, snapshot, grpc.WaitForReady(false))
	})
}
func validKey(s string) bool {
	if len(s) < 1 || len(s) > 128 || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func retry[T any](parent context.Context, token string, call func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	// Replace inherited outgoing authorization rather than appending a second
	// identity. Never copy arbitrary incoming metadata across a trust boundary.
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+token)
	ctx = metadata.NewOutgoingContext(ctx, md)
	var out T
	var err error
	for i := 0; i < 3; i++ {
		if e := ctx.Err(); e != nil {
			return out, status.FromContextError(e).Err()
		}
		attempt, stop := context.WithTimeout(ctx, 750*time.Millisecond)
		out, err = call(attempt)
		stop()
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil {
			return out, status.FromContextError(ctx.Err()).Err()
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return out, err
		}
		if i == 2 {
			break
		}
		// A bounded jittered exponential backoff consumes the original budget.
		delay := time.Duration(15*(1<<i))*time.Millisecond + time.Duration(rand.Int64N(int64(10*time.Millisecond)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return out, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
	return out, err
}
