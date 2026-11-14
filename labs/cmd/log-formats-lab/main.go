package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	logformats "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch06/log-formats"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		f := flag.NewFlagSet("log-formats worker", flag.ContinueOnError)
		path := f.String("job", "", "owned consumer job path")
		if err := f.Parse(os.Args[2:]); err != nil {
			return err
		}
		if *path == "" || f.NArg() != 0 {
			return errors.New("worker requires --job")
		}
		return logformats.RunWorker(ctx, *path)
	}
	f := flag.NewFlagSet("log-formats-lab", flag.ContinueOnError)
	bin := f.String("pg-bin", os.Getenv("ATLAS_LOG_PG_BIN"), "PostgreSQL 18 executable directory")
	share := f.String("pg-share", os.Getenv("ATLAS_LOG_PG_SHARE"), "optional initdb shared file directory")
	work := f.String("workdir", "", "owned temporary database parent")
	output := f.String("output", "", "JSON report path")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected log formats argument")
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	report, err := logformats.RunNative(ctx, logformats.NativeOptions{PGBin: *bin, PGShare: *share, Binary: binary, Workdir: *work})
	if err != nil {
		return err
	}
	if *output != "" {
		if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
			return err
		}
		if err = logformats.Save(*output, report); err != nil {
			return err
		}
	}
	fmt.Printf("log formats: %d consumer runs, %d crash boundaries, %d observations; source/physical/logical state agrees\n", len(report.Consumers), len(report.Crashes), len(report.Observations))
	return nil
}
