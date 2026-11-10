package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	storagebench "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/storage"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/jackc/pgx/v5"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "query-plan experiment:", e)
		os.Exit(1)
	}
}
func run() error {
	rows := flag.Int("rows", 32768, "temporary fixture rows (4096..131072)")
	runs := flag.Int("runs", 3, "warm repetitions per layout (1..5)")
	output := flag.String("output", "artifacts/query-plans.json", "JSON report file")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected argument")
	}
	if e := config.LoadLocalEnv(".env"); e != nil {
		return e
	}
	dsn := os.Getenv("ATLAS_TEST_DATABASE_URL")
	u, e := url.Parse(dsn)
	if e != nil || dsn == "" || u.Path != "/atlas_test" || u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "postgres" {
		return errors.New("ATLAS_TEST_DATABASE_URL must identify the local atlas_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, e := pgx.Connect(ctx, dsn)
	if e != nil {
		return errors.New("could not connect to the local test database")
	}
	defer c.Close(context.Background())
	r, e := storagebench.Run(ctx, c, *rows, *runs)
	if e != nil {
		return e
	}
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(*output), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(*output), ".query-plans-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), *output); e != nil {
		return e
	}
	fmt.Printf("PostgreSQL heap/secondary/composite/covering plans verified; report: %s\n", *output)
	return nil
}
