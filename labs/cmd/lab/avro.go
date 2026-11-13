package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	avrolab "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/avro"
)

func runAvro(args []string) error {
	flags := flag.NewFlagSet("lab avro", flag.ContinueOnError)
	runs := flags.Int("runs", 3, "independent raw sample repetitions (1..5)")
	iterations := flags.Int("iterations", 32, "operations per timed path (1..128)")
	output := flags.String("output", "", "optional JSON report path")
	workdir := flags.String("workdir", "", "parent directory for retained immutable archive datasets")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected Avro lab argument")
	}
	parent := *workdir
	if parent == "" && *output != "" {
		parent = filepath.Dir(*output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	report, e := avrolab.RunAt(ctx, parent, *runs, *iterations)
	if e != nil {
		return e
	}
	b, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if *output != "" {
		if e = saveReport(*output, b); e != nil {
			return e
		}
	}
	_, e = os.Stdout.Write(b)
	return e
}
