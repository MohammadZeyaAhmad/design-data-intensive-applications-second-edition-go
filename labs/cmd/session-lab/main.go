package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	session "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch06/session-consistency"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	f := flag.NewFlagSet("session-lab", flag.ContinueOnError)
	bin := f.String("pg-bin", "", "PostgreSQL 18 executable directory")
	share := f.String("pg-share", "", "optional initdb shared files")
	work := f.String("workdir", "", "owned disposable parent")
	atlas := f.String("atlas", "", "absolute Atlas executable path")
	out := f.String("output", "", "JSON report")
	if e := f.Parse(os.Args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected session-lab argument")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	r, e := session.RunNative(ctx, session.NativeOptions{PGBin: *bin, PGShare: *share, Workdir: *work, Atlas: *atlas})
	if e != nil {
		return e
	}
	if *out != "" {
		if e = os.MkdirAll(filepath.Dir(*out), 0755); e != nil {
			return e
		}
		if e = session.Save(*out, r); e != nil {
			return e
		}
	}
	fmt.Printf("session consistency: %d native observations, %d model scenarios; %d concurrent sessions\n", len(r.Observations), len(r.Model), r.ConcurrentSessions)
	return nil
}
