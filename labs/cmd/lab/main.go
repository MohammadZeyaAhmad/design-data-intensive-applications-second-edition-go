package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/columnstore"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/indexes"
	searchlab "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/storage"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch08/lostupdate"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) > 0 && (args[0] == "lsm" || args[0] == "btree" || args[0] == "tradeoffs" || args[0] == "indexes" || args[0] == "columnstore" || args[0] == "search") {
		flags := flag.NewFlagSet("lab "+args[0], flag.ContinueOnError)
		rows := flags.Int("rows", 512, "synthetic warehouse rows (32..4096)")
		runs := flags.Int("runs", 3, "fresh database repetitions (1..5)")
		output := flags.String("output", "", "optional JSON report path")
		workdir := flags.String("workdir", "", "parent for temporary databases; defaults to report directory or system temp")
		if e := flags.Parse(args[1:]); e != nil {
			return e
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected lab argument")
		}
		parent := *workdir
		if parent == "" && *output != "" {
			parent = filepath.Dir(*output)
		}
		var report any
		var e error
		switch args[0] {
		case "search":
			report, e = searchlab.RunScenarioAt(parent, *rows, *runs)
		case "columnstore":
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			report, e = columnstore.RunScenarioAt(ctx, parent, *rows, *runs)
		case "tradeoffs":
			report, e = storage.RunTradeoffsAt(parent, *rows, *runs)
		case "indexes":
			report, e = indexes.RunScenarioAt(parent, *rows, *runs)
		case "btree":
			report, e = storage.RunBTreeScenarioAt(parent, *rows, *runs)
		default:
			report, e = storage.RunScenarioAt(parent, *rows, *runs)
		}
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
	if len(args) != 1 || args[0] != "lost-update" {
		return errors.New("usage: lab lost-update | lsm|btree|tradeoffs|indexes|columnstore|search [-rows N] [-runs N] [-output path] [-workdir path]")
	}
	report := lostupdate.Run()
	if !report.ExpectedCounterexample || report.UnsafeFinal != 1 || report.AtomicFinal != 2 {
		return errors.New("unexpected model result")
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}
func saveReport(path string, data []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".storage-report-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(data); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
