package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	rpclab "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/rpc"
)

func runRPC(args []string) error {
	flags := flag.NewFlagSet("lab rpc", flag.ContinueOnError)
	binaries := flags.String("binaries", "../artifacts/rpc-binaries", "actual Atlas/router executables and build manifest")
	output := flags.String("output", "", "optional JSON report")
	workdir := flags.String("workdir", "", "parent directory for registry snapshots")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected RPC argument")
	}
	parent := *workdir
	if parent == "" && *output != "" {
		parent = filepath.Dir(*output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	report, e := rpclab.Run(ctx, os.Getenv("ATLAS_TEST_DATABASE_URL"), *binaries, parent)
	if e != nil {
		return e
	}
	raw, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return e
	}
	raw = append(raw, '\n')
	if *output != "" {
		if e = saveReport(*output, raw); e != nil {
			return e
		}
	}
	_, e = os.Stdout.Write(raw)
	return e
}
