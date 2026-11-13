package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/checkpoint"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, "actor node:", e)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: actor-node serve|consume")
	}
	if args[0] == "consume" {
		return consume(ctx, args[1:])
	}
	if args[0] != "serve" {
		return errors.New("unknown actor-node command")
	}
	f := flag.NewFlagSet("actor-node serve", flag.ContinueOnError)
	address := f.String("address", "127.0.0.1:0", "loopback bind")
	reader := f.Int("reader", 2, "reader profile 1 or 2")
	mailbox := f.Int("mailbox", 16, "per-entity queued messages (1..128)")
	before := f.String("pause-before", "", "lab-only message identity")
	after := f.String("pause-after", "", "lab-only message identity")
	mark := f.String("checkpoint", "", "lab-only marker file")
	release := f.String("release", "", "lab-only release file")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return actors.ErrInvalid
	}
	host, _, e := net.SplitHostPort(*address)
	if e != nil || host != "127.0.0.1" {
		return errors.New("actor lab listener must use IPv4 loopback")
	}
	token := os.Getenv("ATLAS_ACTOR_TOKEN")
	tenant := os.Getenv("ATLAS_ACTOR_TENANT")
	if len(token) < 32 || len(token) > 256 || !(actors.Key{Tenant: tenant, SKU: "CHECK"}).Valid() {
		return errors.New("bounded tenant and local actor credential are required")
	}
	o := actors.DefaultOptions()
	o.Reader = *reader
	o.Mailbox = *mailbox
	pause := func(ctx context.Context, id, stage string) error {
		return checkpoint.Pause(ctx, *mark, *release+"."+stage, checkpoint.Event{Stage: stage, ID: id})
	}
	o.Hooks.Before = func(ctx context.Context, m actors.Message) error {
		if *before != "" && m.ID == *before {
			return pause(ctx, m.ID, "before-effect")
		}
		return nil
	}
	o.Hooks.After = func(ctx context.Context, m actors.Message, _ actors.Reply) error {
		if *after != "" && m.ID == *after {
			return pause(ctx, m.ID, "after-effect")
		}
		return nil
	}
	runtime, e := actors.New(ctx, o)
	if e != nil {
		return e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(cleanup)
	}()
	listener, e := net.Listen("tcp", *address)
	if e != nil {
		return e
	}
	defer listener.Close()
	server := &http.Server{Handler: actors.Handler(runtime, tenant, token), ReadHeaderTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case e := <-done:
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e = server.Shutdown(shutdown); e != nil {
			_ = server.Close()
		}
		return nil
	}
}
