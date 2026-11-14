package objectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type BackendConfig struct {
	Kind string   `json:"kind"`
	Root string   `json:"root,omitempty"`
	S3   S3Config `json:"s3,omitempty"`
}

func (c BackendConfig) Open() (Store, error) {
	switch c.Kind {
	case "file":
		return NewFileStore(c.Root)
	case "s3":
		return NewS3Store(c.S3)
	}
	return nil, ErrInvalid
}

type WorkerJob struct {
	Backend BackendConfig `json:"backend"`
	Action  string        `json:"action"`
	Command Command       `json:"command"`
	Phase   string        `json:"phase,omitempty"`
	Ready   string        `json:"ready,omitempty"`
	Release string        `json:"release,omitempty"`
	Output  string        `json:"output"`
	Reads   int           `json:"reads,omitempty"`
}
type ViewEvidence struct {
	Ref    Ref    `json:"ref"`
	Rows   int    `json:"rows"`
	Digest string `json:"rows_sha256"`
}
type WorkerResult struct {
	Outcome     string         `json:"outcome"`
	Publication Publication    `json:"publication"`
	Snapshot    Snapshot       `json:"snapshot"`
	Observed    []ViewEvidence `json:"observed,omitempty"`
	Calls       Counts         `json:"calls"`
}

func applySecrets(c *BackendConfig) {
	c.S3.AccessKey = os.Getenv("ATLAS_OBJECT_S3_ACCESS_KEY")
	c.S3.SecretKey = os.Getenv("ATLAS_OBJECT_S3_SECRET_KEY")
	c.S3.SessionToken = os.Getenv("ATLAS_OBJECT_S3_SESSION_TOKEN")
}
func SaveJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicFile(path, append(b, '\n'))
}
func RunWorker(ctx context.Context, path string) error {
	info, err := os.Stat(path)
	if err != nil || info.Size() > MaxObjectBytes {
		return ErrInvalid
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var job WorkerJob
	if strictJSON(b, &job) != nil || !filepath.IsAbs(job.Output) {
		return ErrInvalid
	}
	applySecrets(&job.Backend)
	store, err := job.Backend.Open()
	if err != nil {
		return err
	}
	meter := &Meter{Store: store}
	cache, _ := NewCache(2 * MaxObjectBytes)
	reader := Reader{meter, cache}
	result := WorkerResult{}
	gate := func(phase string) error {
		if phase != job.Phase {
			return nil
		}
		if !filepath.IsAbs(job.Ready) || !filepath.IsAbs(job.Release) {
			return ErrInvalid
		}
		if err := SaveJSON(job.Ready, map[string]any{"phase": phase, "pid": os.Getpid()}); err != nil {
			return err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(job.Release); err == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
	if job.Action == "read" {
		reads := max(job.Reads, 1)
		if reads > 8 {
			return ErrLimit
		}
		for i := range reads {
			result.Snapshot, err = reader.Current(ctx)
			if err != nil {
				return err
			}
			result.Observed = append(result.Observed, ViewEvidence{result.Snapshot.Ref, len(result.Snapshot.Rows), rowsDigest(result.Snapshot.Rows)})
			if i == 0 && job.Phase != "" {
				if err = gate("read_first"); err != nil {
					return err
				}
			}
			if i+1 < reads {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
		result.Outcome = "read"
	} else if job.Action == "publish" {
		e := Engine{Reader: reader}
		if job.Phase != "" {
			e.Hook = gate
		}
		result.Publication, err = e.Publish(ctx, job.Command)
		result.Outcome = "published"
		if errors.Is(err, ErrConflict) {
			result.Outcome = "conflict"
			err = nil
		}
	} else {
		return ErrInvalid
	}
	if err != nil {
		return err
	}
	result.Calls = meter.Snapshot()
	return SaveJSON(job.Output, result)
}
