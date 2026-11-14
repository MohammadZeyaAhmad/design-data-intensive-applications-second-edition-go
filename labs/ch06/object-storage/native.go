package objectstorage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/childprocess"
)

type RunOptions struct {
	Profile, Binary, Python, ServerScript, Workdir string
	Cloud                                          S3Config
}
type CrashEvidence struct {
	Phase                string `json:"phase"`
	GenerationBefore     uint64 `json:"generation_before"`
	GenerationAfterKill  uint64 `json:"generation_after_kill"`
	RecoveredGeneration  uint64 `json:"recovered_generation"`
	RetryReplayed        bool   `json:"retry_replayed"`
	OrphanCandidates     int    `json:"orphan_candidates_after_kill"`
	SegmentObjectsReused bool   `json:"segment_objects_reused"`
}
type NativeEvidence struct {
	ConcurrentWriterWins      int             `json:"concurrent_writer_wins"`
	ConcurrentWriterConflicts int             `json:"concurrent_writer_conflicts"`
	ReaderObservations        int             `json:"independent_reader_observations"`
	Crashes                   []CrashEvidence `json:"crashes"`
	FinalGeneration           uint64          `json:"final_generation"`
	FinalRows                 int             `json:"final_rows"`
	FinalDigest               string          `json:"final_state_sha256"`
	FinalOrphans              int             `json:"final_orphan_candidates"`
}
type Report struct {
	Profile            string           `json:"profile"`
	Contract           ContractReport   `json:"contract"`
	Workload           WorkloadEvidence `json:"workload"`
	Native             NativeEvidence   `json:"native"`
	ParentCalls        Counts           `json:"parent_process_api_attempts"`
	ParentWire         *WireCounts      `json:"parent_process_s3_http,omitempty"`
	OwnedPrefixCleanup bool             `json:"owned_s3_prefix_cleanup_verified,omitempty"`
	Assumptions        []string         `json:"assumptions"`
}

func nonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func poll(ctx context.Context, what string, condition func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		ok, err := condition()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-timer.C:
		}
	}
}
func Run(ctx context.Context, o RunOptions) (r Report, resultErr error) {
	r = Report{Profile: o.Profile, Assumptions: []string{"strong per-key GET and atomic conditional PUT are required, not inferred from listing", "head publication is the sole visibility point; immutable bodies and all history stay retained", "warehouse snapshots are derived reads, never scarce Inventory write authority", "128 generations, 64 segment references, 4096 live rows; receipt history is never silently evicted", "cache identity is content-addressed; the current head is fetched on every current read", "local fsync/process-kill evidence does not simulate a host power failure", "API counters cover parent-process attempts; killed/completed child requests are not included in those totals", "Moto success validates this emulator contract, not AWS durability, identity policy or cloud availability"}}
	if !filepath.IsAbs(o.Binary) {
		return r, ErrInvalid
	}
	dir, err := os.MkdirTemp(o.Workdir, "atlas-object-")
	if err != nil {
		return r, err
	}
	defer os.RemoveAll(dir)
	config := BackendConfig{Kind: "file", Root: filepath.Join(dir, "file-store")}
	var server *childprocess.Child
	if o.Profile == "emulator" || o.Profile == "cloud" {
		config.Kind = "s3"
		config.Root = ""
		if o.Profile == "emulator" {
			if !filepath.IsAbs(o.Python) || !filepath.IsAbs(o.ServerScript) {
				return r, ErrInvalid
			}
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				return r, e
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			server, err = childprocess.Start(ctx, o.Python, dir, nil, o.ServerScript, strconv.Itoa(port))
			if err != nil {
				return r, err
			}
			defer server.Stop()
			endpoint := "http://127.0.0.1:" + strconv.Itoa(port)
			if err = poll(ctx, "Moto readiness", func() (bool, error) {
				if server.Exited() {
					return false, errors.New("Moto exited; verify the pinned runtime")
				}
				response, e := (&http.Client{Timeout: time.Second}).Get(endpoint + "/moto-api/")
				if e != nil {
					return false, nil
				}
				response.Body.Close()
				return response.StatusCode < 500, nil
			}); err != nil {
				return r, err
			}
			n, err := nonce()
			if err != nil {
				return r, err
			}
			config.S3 = S3Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "atlas-object-" + n[:16], Prefix: "atlas-object-lab/" + n + "/", AccessKey: "atlas-disposable-key", SecretKey: "atlas-disposable-secret"}
		} else {
			config.S3 = o.Cloud
			n, err := nonce()
			if err != nil {
				return r, err
			}
			config.S3.Prefix = "atlas-object-lab/" + n + "/"
		}
	} else if o.Profile != "file" {
		return r, ErrInvalid
	}
	store, err := config.Open()
	if err != nil {
		return r, err
	}
	if s3store, ok := store.(*S3Store); ok {
		if o.Profile == "emulator" {
			if err = s3store.CreateFixtureBucket(ctx); err != nil {
				return r, err
			}
		} else {
			keys, e := s3store.List(ctx, "")
			if e != nil {
				return r, e
			}
			if len(keys) != 0 {
				return r, errors.New("freshly allocated S3 namespace is not empty")
			}
		}
		// Cleanup only the freshly allocated prefix, after all compute children
		// have stopped. Never delete a user bucket or keys outside this prefix.
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			keys, e := s3store.List(cleanup, "")
			if e == nil {
				for _, key := range keys {
					if err := s3store.Delete(cleanup, key); err != nil {
						e = errors.Join(e, err)
					}
				}
			}
			if e == nil {
				remaining, err := s3store.List(cleanup, "")
				e = err
				if e == nil && len(remaining) != 0 {
					e = errors.New("owned prefix is not empty")
				}
			}
			if e != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("owned S3 prefix %s cleanup failed: %w", config.S3.Prefix, e))
			} else {
				r.OwnedPrefixCleanup = true
			}
			counts := s3store.WireCounts()
			r.ParentWire = &counts
		}()
	}
	meter := &Meter{Store: store}
	fmt.Fprintln(os.Stderr, "object storage:", o.Profile, "conditional-write contract")
	r.Contract, err = CheckContract(ctx, meter)
	if err != nil {
		return r, err
	}
	fmt.Fprintln(os.Stderr, "object storage:", o.Profile, "warehouse snapshots and cache")
	r.Workload, err = RunWorkload(ctx, meter)
	if err != nil {
		return r, err
	}
	fmt.Fprintln(os.Stderr, "object storage:", o.Profile, "independent compute and process kills")
	r.Native, err = runNative(ctx, o.Binary, dir, config, meter)
	if err != nil {
		return r, err
	}
	r.ParentCalls = meter.Snapshot()
	if s3store, ok := store.(*S3Store); ok {
		counts := s3store.WireCounts()
		r.ParentWire = &counts
	}
	return r, nil
}

type worker struct {
	process *childprocess.Child
	job     WorkerJob
}

func launchWorker(ctx context.Context, binary, dir, name string, config BackendConfig, command Command, action, phase, release string, reads int) (*worker, error) {
	job := WorkerJob{Backend: config, Action: action, Command: command, Phase: phase, Ready: filepath.Join(dir, name+"-ready.json"), Release: release, Output: filepath.Join(dir, name+"-result.json"), Reads: reads}
	jobPath := filepath.Join(dir, name+"-job.json")
	if err := SaveJSON(jobPath, job); err != nil {
		return nil, err
	}
	env := map[string]string{"ATLAS_OBJECT_S3_ACCESS_KEY": config.S3.AccessKey, "ATLAS_OBJECT_S3_SECRET_KEY": config.S3.SecretKey, "ATLAS_OBJECT_S3_SESSION_TOKEN": config.S3.SessionToken}
	p, err := childprocess.Start(ctx, binary, dir, env, "worker", "--job", jobPath)
	if err != nil {
		return nil, err
	}
	return &worker{p, job}, nil
}
func (w *worker) ready(ctx context.Context) error {
	return poll(ctx, "compute boundary", func() (bool, error) {
		_, err := os.Stat(w.job.Ready)
		if err == nil {
			return true, nil
		}
		if w.process.Exited() {
			return false, errors.New("compute process exited before boundary")
		}
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	})
}
func (w *worker) result(ctx context.Context) (WorkerResult, error) {
	if err := w.process.Wait(ctx); err != nil {
		return WorkerResult{}, fmt.Errorf("compute worker failed: %w", err)
	}
	info, err := os.Stat(w.job.Output)
	if err != nil || info.Size() > MaxObjectBytes {
		return WorkerResult{}, errors.New("compute worker output missing or unbounded")
	}
	b, err := os.ReadFile(w.job.Output)
	if err != nil {
		return WorkerResult{}, err
	}
	var r WorkerResult
	if strictJSON(b, &r) != nil {
		return r, ErrCorrupt
	}
	return r, nil
}
func withRow(rows []Record, row Record) []Record {
	result := append(append([]Record(nil), rows...), row)
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}
func runNative(ctx context.Context, binary, dir string, config BackendConfig, meter *Meter) (NativeEvidence, error) {
	r := NativeEvidence{}
	e := newEngine(meter)
	base, err := e.Reader.Current(ctx)
	if err != nil {
		return r, err
	}
	release := filepath.Join(dir, "race-release")
	commands := []Command{{"compute-left", "update", []Record{{Key: "acme/east/COMPUTELEFT", Value: 71}}}, {"compute-right", "update", []Record{{Key: "globex/west/COMPUTERIGHT", Value: 83}}}}
	workers := []*worker{}
	for i, c := range commands {
		w, err := launchWorker(ctx, binary, dir, fmt.Sprint("writer-", i), config, c, "publish", "before_head", release, 0)
		if err != nil {
			return r, err
		}
		defer w.process.Stop()
		workers = append(workers, w)
	}
	for _, w := range workers {
		if err = w.ready(ctx); err != nil {
			return r, err
		}
	}
	observer, err := launchWorker(ctx, binary, dir, "race-reader", config, Command{}, "read", "read_first", release, 8)
	if err != nil {
		return r, err
	}
	defer observer.process.Stop()
	if err = observer.ready(ctx); err != nil {
		return r, err
	}
	if err = os.WriteFile(release, nil, 0600); err != nil {
		return r, err
	}
	for _, w := range workers {
		result, err := w.result(ctx)
		if err != nil {
			return r, err
		}
		switch result.Outcome {
		case "published":
			r.ConcurrentWriterWins++
		case "conflict":
			r.ConcurrentWriterConflicts++
		default:
			return r, ErrCorrupt
		}
	}
	if r.ConcurrentWriterWins != 1 || r.ConcurrentWriterConflicts != 1 {
		return r, errors.New("independent same-generation writers did not expose one CAS conflict")
	}
	observed, err := observer.result(ctx)
	if err != nil {
		return r, err
	}
	allowed := map[string]bool{rowsDigest(base.Rows): true, rowsDigest(withRow(base.Rows, commands[0].Changes[0])): true, rowsDigest(withRow(base.Rows, commands[1].Changes[0])): true}
	for _, view := range observed.Observed {
		if !allowed[view.Digest] {
			return r, errors.New("independent reader observed a partial generation")
		}
	}
	r.ReaderObservations = len(observed.Observed)
	if r.ReaderObservations != 8 {
		return r, ErrCorrupt
	}
	for i, c := range commands {
		w, err := launchWorker(ctx, binary, dir, fmt.Sprint("rebase-", i), config, c, "publish", "", "", 0)
		if err != nil {
			return r, err
		}
		defer w.process.Stop()
		if _, err = w.result(ctx); err != nil {
			return r, err
		}
	}
	current, err := e.Reader.Current(ctx)
	if err != nil || current.Ref.Generation != base.Ref.Generation+2 || len(current.Rows) != len(base.Rows)+2 {
		return r, errors.New("explicit rebase omitted a competing command")
	}
	for i, phase := range []string{"segments", "manifest", "before_head", "after_head"} {
		before, err := e.Reader.Current(ctx)
		if err != nil {
			return r, err
		}
		command := Command{fmt.Sprint("crash-", i), "update", []Record{{Key: fmt.Sprintf("acme/east/CRASH%d", i), Value: int64(900 + i)}}}
		w, err := launchWorker(ctx, binary, dir, fmt.Sprint("crash-", i), config, command, "publish", phase, filepath.Join(dir, fmt.Sprint("never-release-", i)), 0)
		if err != nil {
			return r, err
		}
		defer w.process.Stop()
		if err = w.ready(ctx); err != nil {
			return r, err
		}
		w.process.Kill()
		if _, err = os.Stat(w.job.Output); !os.IsNotExist(err) {
			return r, errors.New("killed process unexpectedly acknowledged")
		}
		after, err := e.Reader.Current(ctx)
		if err != nil {
			return r, err
		}
		expectedGen := before.Ref.Generation
		if phase == "after_head" {
			expectedGen++
		}
		if after.Ref.Generation != expectedGen {
			return r, errors.New("compute crash exposed wrong generation")
		}
		if phase != "after_head" && !reflect.DeepEqual(before.Rows, after.Rows) {
			return r, errors.New("unpublished object leaked into read view")
		}
		audit, err := e.Audit(ctx)
		if err != nil {
			return r, err
		}
		if phase != "after_head" && len(audit.Orphans) == 0 {
			return r, errors.New("prepublication crash did not leave inventoryable orphans")
		}
		segments, err := meter.List(ctx, "segments/")
		if err != nil {
			return r, err
		}
		recovered, err := e.Publish(ctx, command)
		if err != nil {
			return r, err
		}
		afterSegments, err := meter.List(ctx, "segments/")
		if err != nil {
			return r, err
		}
		if len(segments) != len(afterSegments) {
			return r, errors.New("retry failed to reuse immutable segment")
		}
		final, err := e.Reader.Current(ctx)
		if err != nil {
			return r, err
		}
		expectedRows := withRow(before.Rows, command.Changes[0])
		if !reflect.DeepEqual(final.Rows, expectedRows) || final.Ref.Generation != before.Ref.Generation+1 {
			return r, errors.New("recovered command duplicated or lost warehouse state")
		}
		read, err := launchWorker(ctx, binary, dir, fmt.Sprint("reopened-reader-", i), config, Command{}, "read", "", "", 1)
		if err != nil {
			return r, err
		}
		defer read.process.Stop()
		reopened, err := read.result(ctx)
		if err != nil || !reflect.DeepEqual(reopened.Snapshot, final) {
			return r, errors.New("independent reopened reader disagrees")
		}
		r.Crashes = append(r.Crashes, CrashEvidence{phase, before.Ref.Generation, after.Ref.Generation, recovered.Ref.Generation, recovered.Replayed, len(audit.Orphans), true})
	}
	final, err := e.Reader.Current(ctx)
	if err != nil {
		return r, err
	}
	audit, err := e.Audit(ctx)
	if err != nil {
		return r, err
	}
	r.FinalGeneration = final.Ref.Generation
	r.FinalRows = len(final.Rows)
	r.FinalDigest = rowsDigest(final.Rows)
	r.FinalOrphans = len(audit.Orphans)
	return r, nil
}
