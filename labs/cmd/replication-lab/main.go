package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	singleleader "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch06/single-leader"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && (os.Args[1] == "wal-archive" || os.Args[1] == "wal-restore") {
		if len(os.Args) != 5 {
			return errors.New("WAL helper requires directory, source/name, name/destination")
		}
		if os.Args[1] == "wal-archive" {
			return singleleader.Archive(os.Args[2], os.Args[3], os.Args[4])
		}
		return singleleader.Restore(os.Args[2], os.Args[3], os.Args[4])
	}
	f := flag.NewFlagSet("replication-lab", flag.ContinueOnError)
	pg := f.String("pg-bin", os.Getenv("ATLAS_REPLICATION_PG_BIN"), "directory containing PostgreSQL 18 binaries")
	share := f.String("pg-share", os.Getenv("ATLAS_REPLICATION_PG_SHARE"), "optional PostgreSQL shared files")
	atlas := f.String("atlas", "", "actual Atlas executable")
	workdir := f.String("workdir", "", "parent for owned temporary clusters")
	output := f.String("output", "", "JSON evidence file")
	model := f.Bool("model-only", false, "run only the bounded durability model")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected replication argument")
	}
	var report any
	if *model {
		r, err := singleleader.Demonstrate()
		if err != nil {
			return err
		}
		report = r
	} else {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		helper, err := os.Executable()
		if err != nil {
			return err
		}
		r, err := singleleader.RunNative(ctx, singleleader.NativeOptions{PGBin: *pg, PGShare: *share, Atlas: *atlas, Helper: helper, Workdir: *workdir})
		if err != nil {
			return err
		}
		report = r
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if *output != "" {
		if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
			return err
		}
		if err = saveReport(*output, b); err != nil {
			return err
		}
	}
	fmt.Print(string(b))
	return nil
}

// Publish atomically so a repeat run from a different disposable container UID
// can replace the evidence without depending on the old file's ownership.
func saveReport(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".replication-report-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Chmod(0644); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
