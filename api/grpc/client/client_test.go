package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stub struct {
	pb.InventoryClient
	calls atomic.Int32
	code  codes.Code
}

func (s *stub) Reserve(ctx context.Context, r *pb.ReserveRequest, _ ...grpc.CallOption) (*pb.ReservationReply, error) {
	s.calls.Add(1)
	return nil, status.Error(s.code, "fixture")
}
func TestRetriesAreBoundedAndRejectUnsafeIdentity(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.AlreadyExists, codes.FailedPrecondition, codes.InvalidArgument} {
		s := &stub{code: code}
		c := Client{RPC: s}
		_, e := c.Reserve(context.Background(), &pb.ReserveRequest{IdempotencyKey: "stable"})
		expected := int32(1)
		if code == codes.Unavailable {
			expected = 3
		}
		if status.Code(e) != code || s.calls.Load() != expected {
			t.Fatalf("code %s: calls=%d error=%v", code, s.calls.Load(), e)
		}
	}
	s := &stub{code: codes.Unavailable}
	_, e := (Client{RPC: s}).Reserve(context.Background(), &pb.ReserveRequest{})
	if status.Code(e) != codes.InvalidArgument || s.calls.Load() != 0 {
		t.Fatal("missing identity reached transport")
	}
}
func TestCanceledParentPreventsRetries(t *testing.T) {
	s := &stub{code: codes.Unavailable}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	_, e := (Client{RPC: s}).Reserve(ctx, &pb.ReserveRequest{IdempotencyKey: "stable"})
	if status.Code(e) != codes.DeadlineExceeded || s.calls.Load() != 0 {
		t.Fatalf("parent budget ignored: %v, %d", e, s.calls.Load())
	}
}
