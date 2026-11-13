package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/drill"
)

func runActors(args []string, broker bool) error {
	flags := flag.NewFlagSet("lab actors", flag.ContinueOnError)
	binaries := flags.String("binaries", "../artifacts/actors-binaries", "actual actor-node executable and manifest")
	output := flags.String("output", "", "optional JSON report")
	workdir := flags.String("workdir", "", "retained checkpoint parent directory")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected actor argument")
	}
	parent := *workdir
	if parent == "" && *output != "" {
		parent = filepath.Dir(*output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	var report drill.Report
	var e error
	if broker {
		address := os.Getenv("ATLAS_ACTORS_BROKERS")
		if address == "" {
			address = "127.0.0.1:19094"
		}
		report, e = drill.RunBroker(ctx, os.Getenv("ATLAS_TEST_DATABASE_URL"), address, *binaries, parent)
	} else {
		report, e = drill.RunLocal(ctx, *binaries, parent)
	}
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
