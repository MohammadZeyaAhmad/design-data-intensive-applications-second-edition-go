package rpc

import (
	"testing"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/rpc/wire/inventory/v1"
	"google.golang.org/protobuf/proto"
)

func TestFrozenDescriptorPreservesUnknownRepliesAndExposesFutureRequests(t *testing.T) {
	current := &pb.ReservationReply{Reservation: &pb.Reservation{Id: "original", WarehouseId: "north"}, ProtocolRevision: 2, Replayed: true}
	raw, e := proto.Marshal(current)
	if e != nil {
		t.Fatal(e)
	}
	old, e := legacyMessage("ReservationReply")
	if e != nil {
		t.Fatal(e)
	}
	if e = proto.Unmarshal(raw, old); e != nil {
		t.Fatal(e)
	}
	raw, e = proto.Marshal(old)
	if e != nil {
		t.Fatal(e)
	}
	restored := &pb.ReservationReply{}
	if e = proto.Unmarshal(raw, restored); e != nil {
		t.Fatal(e)
	}
	if !proto.Equal(current, restored) {
		t.Fatal("old binary relay lost new response semantics")
	}
	request := &pb.ReserveRequest{IdempotencyKey: "key", Sku: "RPC", Quantity: 1, WarehouseId: "north"}
	raw, e = proto.Marshal(request)
	if e != nil {
		t.Fatal(e)
	}
	baseline, e := legacyMessage("ReserveRequest")
	if e != nil {
		t.Fatal(e)
	}
	if e = proto.Unmarshal(raw, baseline); e != nil {
		t.Fatal(e)
	}
	if len(baseline.GetUnknown()) == 0 {
		t.Fatal("old adapter cannot detect unsupported warehouse semantics")
	}
}
func TestOpenAPIUsesExactBoundsAndRejectsUnsupportedInputs(t *testing.T) {
	if e := validateREST("ReserveRequest", []byte(`{"sku":"RPC","quantity":1}`)); e != nil {
		t.Fatal(e)
	}
	for _, raw := range []string{`{"sku":"RPC","quantity":1000000001}`, `{"sku":"RPC","quantity":1,"ttl_seconds":3601}`, `{"sku":"RPC","quantity":1,"tenant_id":"other"}`, `{"sku":"RPC","quantity":1,"future_behavior":true}`} {
		if validateREST("ReserveRequest", []byte(raw)) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
