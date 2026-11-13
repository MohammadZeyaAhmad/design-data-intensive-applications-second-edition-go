package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/evolution"
)

func runEvolution(args []string) error {
	flags := flag.NewFlagSet("lab evolution", flag.ContinueOnError)
	rows := flags.Int("rows", 128, "historical brands (32..512)")
	binaries := flags.String("binaries", "../artifacts/evolution-binaries", "pinned historical/current binary directory")
	output := flags.String("output", "", "optional JSON report path")
	workdir := flags.String("workdir", "", "parent directory for retained immutable archive")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected evolution argument")
	}
	parent := *workdir
	if parent == "" && *output != "" {
		parent = filepath.Dir(*output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	report, e := evolution.Run(ctx, os.Getenv("ATLAS_TEST_DATABASE_URL"), *binaries, parent, *rows)
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
