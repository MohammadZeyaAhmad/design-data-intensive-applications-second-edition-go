package drill

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/childprocess"
)

type node struct {
	child   *childprocess.Child
	client  *actors.Client
	address string
}

func startNode(ctx context.Context, binary, dir, token, address string, reader int, marker, release string) (*node, error) {
	if address == "" {
		var e error
		address, e = freeAddress()
		if e != nil {
			return nil, e
		}
	}
	args := []string{"serve", "-address", address, "-reader", fmt.Sprint(reader), "-mailbox", "1"}
	if marker != "" {
		args = append(args, "-pause-before", "blocked", "-pause-after", "lost-response", "-checkpoint", marker, "-release", release)
	}
	child, e := childprocess.Start(ctx, binary, dir, map[string]string{"ATLAS_ACTOR_TOKEN": token, "ATLAS_ACTOR_TENANT": "acme"}, args...)
	if e != nil {
		return nil, e
	}
	client, e := actors.NewClient("http://"+address, token)
	if e != nil {
		child.Stop()
		return nil, e
	}
	if e = wait(ctx, 10*time.Second, func() bool { return !child.Exited() && client.Ready(ctx) }); e != nil {
		child.Stop()
		return nil, e
	}
	return &node{child: child, client: client, address: address}, nil
}
func raw(writer int, id string, key actors.Key, kind string, quantity int64) ([]byte, error) {
	return actors.Encode(writer, id, key, kind, quantity, "v2 annotation")
}
func query(ctx context.Context, sender actors.Sender, ref actors.Ref, id string) (actors.Reply, error) {
	b, e := raw(2, id, ref.Key, "get.v1", 0)
	if e != nil {
		return actors.Reply{}, e
	}
	return sender.Ask(ctx, ref, b)
}
func RunLocal(ctx context.Context, binaries, parent string) (report Report, runErr error) {
	report.Scenario = "actors-transient"
	stage := "prepare"
	defer func() {
		if runErr != nil {
			runErr = fmt.Errorf("actor %s: %w", stage, runErr)
		}
	}()
	binary, dir, build, e := prepare(binaries, parent)
	if e != nil {
		return report, e
	}
	report.Build = build
	secret, e := nonce()
	if e != nil {
		return report, e
	}
	token := "actor-local-token-" + secret
	stage = "local supervision"
	if e = localModel(ctx, &report); e != nil {
		return report, e
	}
	stage = "native node startup"
	left, e := startNode(ctx, binary, dir, token, "", 1, "", "")
	if e != nil {
		return report, e
	}
	defer left.child.Stop()
	marker, release := filepath.Join(dir, "checkpoint.json"), filepath.Join(dir, "release")
	right, e := startNode(ctx, binary, dir, token, "", 2, marker, release)
	if e != nil {
		return report, e
	}
	defer right.child.Stop()
	key := actors.Key{Tenant: "acme", SKU: "BOOK"}
	leftRef, e := left.client.Resolve(ctx, key)
	if e != nil {
		return report, e
	}
	rightRef, e := right.client.Resolve(ctx, key)
	if e != nil {
		return report, e
	}
	stage = "rolling writer/reader matrix"
	samples := []map[string]any{}
	for reader, n := range []*node{left, right} {
		ref := leftRef
		if reader == 1 {
			ref = rightRef
		}
		for writer := 1; writer <= 2; writer++ {
			wire, e := raw(writer, fmt.Sprintf("writer-%d-reader-%d", writer, reader+1), key, "receive.v1", 1)
			if e != nil {
				return report, e
			}
			out, e := n.client.Ask(ctx, ref, wire)
			if e != nil {
				return report, e
			}
			noteExpected := writer == 2 && reader == 1
			if (out.Note != "") == noteExpected {
				samples = append(samples, map[string]any{"writer": writer, "reader": reader + 1, "version": out.State.Version, "note": out.Note})
			} else {
				return report, errors.New("writer/reader profile changed common semantics")
			}
		}
	}
	if e = report.check("four real writer/reader message combinations", "all common stock transitions work; only current reader exposes optional annotation", samples, len(samples) == 4); e != nil {
		return report, e
	}
	wire, e := raw(2, "left-only", key, "receive.v1", 3)
	if e != nil {
		return report, e
	}
	l, e := left.client.Ask(ctx, leftRef, wire)
	if e != nil {
		return report, e
	}
	r, e := query(ctx, right.client, rightRef, "right-state")
	if e != nil {
		return report, e
	}
	if e = report.check("actor ownership is local, not cluster consensus", "same entity has independent state in two nodes", []int64{l.State.OnHand, r.State.OnHand}, l.State.OnHand == 5 && r.State.OnHand == 2); e != nil {
		return report, e
	}
	stage = "lost remote Ask response"
	wire, e = raw(2, "lost-response", key, "receive.v1", 4)
	if e != nil {
		return report, e
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, callErr := right.client.Ask(short, rightRef, wire)
	cancel()
	if e = waitCheckpoint(ctx, marker, "after-effect", "lost-response"); e != nil {
		return report, e
	}
	if e = report.check("remote Ask times out after accepted effect", "deadline exceeded after actual actor mutation", fmt.Sprint(callErr), errors.Is(callErr, context.DeadlineExceeded)); e != nil {
		return report, e
	}
	if e = os.WriteFile(release+".after-effect", []byte("release"), 0600); e != nil {
		return report, e
	}
	replayed, e := right.client.Ask(ctx, rightRef, wire)
	if e != nil {
		return report, e
	}
	if e = report.check("same-generation receipt resolves lost reply", "saved reply; stock changed once", replayed, replayed.Replayed && replayed.State.OnHand == 6); e != nil {
		return report, e
	}
	stage = "accepted Tell and bounded queue"
	before, e := right.client.Diagnostics(ctx)
	if e != nil {
		return report, e
	}
	block, e := raw(2, "blocked", key, "receive.v1", 1)
	if e != nil {
		return report, e
	}
	if _, e = right.client.Tell(ctx, rightRef, block); e != nil {
		return report, e
	}
	if e = waitCheckpoint(ctx, marker, "before-effect", "blocked"); e != nil {
		return report, e
	}
	queued, e := raw(2, "queued", key, "receive.v1", 9)
	if e != nil {
		return report, e
	}
	if _, e = right.client.Tell(ctx, rightRef, queued); e != nil {
		return report, e
	}
	overflow, e := raw(2, "overflow", key, "receive.v1", 9)
	if e != nil {
		return report, e
	}
	_, overflowErr := right.client.Tell(ctx, rightRef, overflow)
	after, e := right.client.Diagnostics(ctx)
	if e != nil {
		return report, e
	}
	if e = report.check("Tell admission is bounded and distinct from completion", "two admitted; third rejected while actor has not processed them", after.Stats, after.Stats.Accepted-before.Stats.Accepted == 2 && errors.Is(overflowErr, actors.ErrBackpressure)); e != nil {
		return report, e
	}
	otherKey := actors.Key{Tenant: "acme", SKU: "OTHER"}
	otherRef, e := right.client.Resolve(ctx, otherKey)
	if e != nil {
		return report, e
	}
	independent, e := raw(2, "independent", otherKey, "receive.v1", 7)
	if e != nil {
		return report, e
	}
	other, e := right.client.Ask(ctx, otherRef, independent)
	if e != nil {
		return report, e
	}
	if e = report.check("blocked entity does not block another actor", "independent stock received while BOOK remains blocked", other.State, other.State.OnHand == 7); e != nil {
		return report, e
	}
	stage = "actual node SIGKILL and incarnation fence"
	right.child.Kill()
	replacement, e := startNode(ctx, binary, dir, token, right.address, 2, "", "")
	if e != nil {
		return report, e
	}
	defer replacement.child.Stop()
	fresh, e := replacement.client.Resolve(ctx, key)
	if e != nil {
		return report, e
	}
	state, e := query(ctx, replacement.client, fresh, "after-restart")
	if e != nil {
		return report, e
	}
	if e = report.check("SIGKILL loses actor memory and queued transient messages", "zero stock/version after actual process replacement", state, state.State.OnHand == 0 && state.State.Version == 0); e != nil {
		return report, e
	}
	stale, e := raw(2, "old-incarnation", key, "receive.v1", 1)
	if e != nil {
		return report, e
	}
	_, staleErr := replacement.client.Tell(ctx, rightRef, stale)
	if e = report.check("old references cannot address a new process incarnation", "different boot identity; stale reference rejected", map[string]any{"different": fresh.Incarnation != rightRef.Incarnation, "error": fmt.Sprint(staleErr)}, fresh.Incarnation != rightRef.Incarnation && errors.Is(staleErr, actors.ErrStale)); e != nil {
		return report, e
	}
	report.Verified = true
	report.Boundaries = []string{"Local/reference scheduling is not distributed ownership or durable storage.", "Tell acknowledges bounded volatile admission. Ask cancellation stops waiting; accepted work may continue.", "The finite receipt cache and dead-letter ring disappear with actor/process state.", "Actor-node is a loopback-only lab executable with lab-only file checkpoints, not a production application endpoint."}
	return report, nil
}
func localModel(ctx context.Context, report *Report) error {
	o := actors.DefaultOptions()
	o.MaxRestarts = 1
	o.RetainedReceipts = 1
	o.Hooks.Before = func(_ context.Context, m actors.Message) error {
		if m.ID == "boom-one" || m.ID == "boom-two" {
			panic("controlled supervised actor failure")
		}
		return nil
	}
	runtime, e := actors.New(ctx, o)
	if e != nil {
		return e
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(stop)
	}()
	sender := actors.Local{Runtime: runtime}
	key := actors.Key{Tenant: "acme", SKU: "MODEL"}
	ref, e := sender.Resolve(ctx, key)
	if e != nil {
		return e
	}
	first, e := raw(2, "first", key, "receive.v1", 1)
	if e != nil {
		return e
	}
	if _, e = sender.Ask(ctx, ref, first); e != nil {
		return e
	}
	twice, e := sender.Ask(ctx, ref, first)
	if e != nil {
		return e
	}
	if e = report.check("local transport returns a saved receipt", "same ID/payload replays while retained", twice.Replayed, twice.Replayed); e != nil {
		return e
	}
	evict, e := raw(2, "evict", key, "receive.v1", 1)
	if e != nil {
		return e
	}
	if _, e = sender.Ask(ctx, ref, evict); e != nil {
		return e
	}
	again, e := sender.Ask(ctx, ref, first)
	if e != nil {
		return e
	}
	if e = report.check("bounded memory receipts have a finite horizon", "evicted identity applies again", again, again.State.OnHand == 3 && !again.Replayed); e != nil {
		return e
	}
	crash, e := raw(2, "boom-one", key, "receive.v1", 1)
	if e != nil {
		return e
	}
	_, crashErr := sender.Ask(ctx, ref, crash)
	var fresh actors.Ref
	if e = wait(ctx, time.Second, func() bool { fresh, e = sender.Resolve(ctx, key); return e == nil && fresh.Generation == 2 }); e != nil {
		return e
	}
	state, e := query(ctx, sender, fresh, "after-supervision")
	if e != nil {
		return e
	}
	if e = report.check("supervisor restarts with new generation and empty state", "one supervised restart; memory reset", runtime.Stats(), errors.Is(crashErr, actors.ErrCrashed) && state.State.OnHand == 0 && runtime.Stats().Restarts == 1); e != nil {
		return e
	}
	_, staleErr := sender.Tell(ctx, ref, first)
	if e = report.check("supervision fences old generation references", "stale local reference rejected", fmt.Sprint(staleErr), errors.Is(staleErr, actors.ErrStale)); e != nil {
		return e
	}
	crash, e = raw(2, "boom-two", key, "receive.v1", 1)
	if e != nil {
		return e
	}
	_, e = sender.Ask(ctx, fresh, crash)
	if !errors.Is(e, actors.ErrCrashed) {
		return e
	}
	if e = wait(ctx, time.Second, func() bool { _, e := sender.Resolve(ctx, key); return errors.Is(e, actors.ErrStopped) }); e != nil {
		return e
	}
	return report.check("restart budget stops a repeated poison actor", "actor remains stopped after its configured restart budget", runtime.Stats(), runtime.Stats().Restarts == 1 && runtime.Stats().Dropped == 2)
}
