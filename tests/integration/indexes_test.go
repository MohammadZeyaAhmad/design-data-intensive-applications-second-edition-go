//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	storagebench "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/storage"
	"github.com/jackc/pgx/v5"
)

func TestPostgresCompositeAndCoveringPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, e := pgx.Connect(ctx, pool.Config().ConnConfig.ConnString())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(context.Background())
	r, e := storagebench.Run(ctx, c, 8192, 2)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || !r.MutationVerified || len(r.Cases) != 4 {
		t.Fatal("incomplete query-plan experiment")
	}
	for _, p := range r.Cases {
		if len(p.Plans) != 2 || p.Rows != 40 || p.Digest != r.Cases[0].Digest {
			t.Fatal("query plans disagree", p.Name)
		}
	}
}
