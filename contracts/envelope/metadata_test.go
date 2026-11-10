package envelope

import (
	"context"
	"testing"
)

func TestWorkerLineageAndLegacyProvenance(t *testing.T) {
	request := "11111111-1111-4111-8111-111111111111"
	cause := "22222222-2222-4222-8222-222222222222"
	parent := Metadata{Version: 1, Actor: "tenant-key:acme", CorrelationID: request, CausationID: request, OriginRequestID: request}
	ctx, err := Worker(context.Background(), parent, "checkout", cause)
	if err != nil {
		t.Fatal(err)
	}
	m := From(ctx)
	if m.Actor != "worker:checkout" || m.CorrelationID != request || m.OriginRequestID != request || m.CausationID != cause {
		t.Fatal("lineage lost", m)
	}
	legacy, err := Record(context.Background(), "system:internal", cause)
	if err != nil || legacy.Version != 1 || legacy.CorrelationID != cause || legacy.OriginRequestID != "" {
		t.Fatal("legacy provenance invented", legacy, err)
	}
	if (Metadata{Version: 0, Actor: "operator:local"}).Valid() {
		t.Fatal("partially populated legacy envelope accepted")
	}
}
