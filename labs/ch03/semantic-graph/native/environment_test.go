package native

import (
	"context"
	"strings"
	"testing"
)

func TestRejectDestructiveOrRedirectedDatabaseBindings(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://atlas@remote/atlas_test?sslmode=disable",
		"postgresql://atlas@127.0.0.1/atlas?sslmode=disable",
		"postgresql://atlas@127.0.0.1/atlas_test?sslmode=disable&host=remote",
		"postgresql://atlas@127.0.0.1/atlas_test?service=other",
		"postgresql://atlas@127.0.0.1/atlas_test?sslmode=disable#ignored",
	} {
		t.Setenv("ATLAS_RDF_POSTGRES_URL", dsn)
		if _, e := Open(context.Background()); e == nil || !strings.Contains(e.Error(), "without connection overrides") {
			t.Fatalf("binding was not rejected before any connection: %v", e)
		}
	}
}
