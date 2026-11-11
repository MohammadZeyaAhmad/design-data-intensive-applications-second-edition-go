package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	experiment "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/graphql"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	output := flag.String("output", "artifacts/graphql.json", "comparison report path")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, e := experiment.Run(ctx)
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
	if e = os.WriteFile(*output, b, 0600); e != nil {
		return e
	}
	_, e = os.Stdout.Write(b)
	return e
}
