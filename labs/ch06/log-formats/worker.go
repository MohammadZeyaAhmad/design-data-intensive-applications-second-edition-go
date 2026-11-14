package logformats

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type WorkerJob struct {
	Identity       Identity `json:"identity"`
	Marker         string   `json:"marker"`
	Phase          string   `json:"phase,omitempty"`
	Ready          string   `json:"ready,omitempty"`
	Release        string   `json:"release,omitempty"`
	Output         string   `json:"output"`
	ReplayRetained bool     `json:"replay_retained"`
}
type WorkerResult struct {
	Outcome string         `json:"outcome"`
	Report  ConsumerReport `json:"report"`
	Error   string         `json:"error,omitempty"`
}

func Save(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".log-report-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(append(body, '\n')); err != nil {
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
func RunWorker(ctx context.Context, path string) error {
	info, err := os.Stat(path)
	if err != nil || info.Size() > 65536 {
		return ErrProtocol
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var job WorkerJob
	if json.Unmarshal(body, &job) != nil || !filepath.IsAbs(job.Output) {
		return ErrProtocol
	}
	o := ConsumerOptions{SourceDSN: os.Getenv("ATLAS_LOG_SOURCE_URL"), TargetDSN: os.Getenv("ATLAS_LOG_TARGET_URL"), Identity: job.Identity, Marker: job.Marker, ReplayRetained: job.ReplayRetained}
	if job.Phase != "" {
		o.Hook = func(phase string, batch Transaction) error {
			if phase != job.Phase {
				return nil
			}
			if !filepath.IsAbs(job.Ready) || !filepath.IsAbs(job.Release) {
				return ErrProtocol
			}
			if err := Save(job.Ready, map[string]any{"phase": phase, "end_lsn": batch.End, "pid": os.Getpid()}); err != nil {
				return err
			}
			timer := time.NewTicker(10 * time.Millisecond)
			defer timer.Stop()
			for {
				if _, err := os.Stat(job.Release); err == nil {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
	report, err := Consume(ctx, o)
	result := WorkerResult{Outcome: "complete", Report: report}
	if errors.Is(err, ErrSchema) {
		result.Outcome = "schema_mismatch"
		err = nil
	}
	if errors.Is(err, ErrSource) {
		result.Outcome = "source_mismatch"
		err = nil
	}
	if err != nil {
		result.Outcome = "failed"
		var p *pgconn.PgError
		if errors.As(err, &p) {
			result.Error = p.Code + ": " + p.Message
		} else if errors.Is(err, ErrProtocol) || errors.Is(err, ErrProjection) || errors.Is(err, ErrLimit) {
			result.Error = err.Error()
		} else {
			result.Error = "logical stream or target operation failed"
		}
		_ = Save(job.Output, result)
		return errors.New(result.Error)
	}
	return Save(job.Output, result)
}
