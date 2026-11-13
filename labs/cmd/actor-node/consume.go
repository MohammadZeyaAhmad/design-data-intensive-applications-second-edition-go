package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/broker"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/durable"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/disposabledatabase"
)

func consume(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("actor-node consume", flag.ContinueOnError)
	c := broker.Config{}
	f.StringVar(&c.Address, "broker", "127.0.0.1:19094", "local Kafka address")
	f.StringVar(&c.Topic, "topic", "", "owned command topic")
	f.StringVar(&c.Replies, "replies", "", "owned reply topic")
	f.StringVar(&c.Group, "group", "", "owned consumer subscription")
	f.StringVar(&c.Name, "name", "worker", "worker label")
	f.IntVar(&c.Reader, "reader", 2, "reader profile")
	f.StringVar(&c.Ready, "ready", "", "assignment snapshot path")
	f.StringVar(&c.BeforeCommitID, "pause-before-commit", "", "lab-only fault target")
	f.StringVar(&c.AfterCommitID, "pause-after-commit", "", "lab-only fault target")
	f.StringVar(&c.Checkpoint, "checkpoint", "", "lab-only checkpoint")
	f.StringVar(&c.Release, "release", "", "lab-only release file")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected actor consumer argument")
	}
	pool, e := disposabledatabase.Open(ctx, os.Getenv("ATLAS_ACTORS_DATABASE_URL"), c.Name)
	if e != nil {
		return e
	}
	defer pool.Close()
	c.Store = durable.Store{Pool: pool}
	return broker.Run(ctx, c)
}
