package lsm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProcessCrashHelper(t *testing.T) {
	if os.Getenv("ATLAS_LSM_CRASH_CHILD") != "1" {
		return
	}
	point := os.Getenv("ATLAS_LSM_CRASH_POINT")
	o := Options{DisableAutoCompaction: true, Failpoint: func(p string) error {
		if p == point {
			fmt.Println("READY")
			for {
				time.Sleep(time.Hour)
			}
		}
		return nil
	}}
	d := mustOpen(t, os.Getenv("ATLAS_LSM_CRASH_DIR"), o)
	for i, op := range powerOperations[:3] {
		if op.deleted {
			must(t, d.Delete(op.key))
		} else {
			must(t, d.Put(op.key, op.value))
		}
		fmt.Println("ACK", i+1)
		if i == 1 {
			must(t, d.Flush())
		}
	}
	must(t, d.Compact())
	must(t, d.Close())
	t.Fatal("requested crash point was not reached")
}

func TestRealProcessKillAtDurabilityBoundaries(t *testing.T) {
	points := []string{
		"wal.written", "wal.synced",
		"flush.table.written", "flush.table.synced", "flush.table.renamed", "flush.table.dir_synced",
		"flush.manifest.written", "flush.manifest.synced", "flush.manifest.renamed", "flush.manifest.dir_synced",
		"flush.wal.truncated", "flush.wal.synced",
		"compact.table.written", "compact.table.synced", "compact.table.renamed", "compact.table.dir_synced",
		"compact.manifest.written", "compact.manifest.synced", "compact.manifest.renamed", "compact.manifest.dir_synced", "compact.obsolete.removed",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, Options{DisableAutoCompaction: true})
			must(t, d.Put([]byte("a"), []byte("base")))
			must(t, d.Put([]byte("b"), []byte("base")))
			must(t, d.Flush())
			must(t, d.Put([]byte("c"), []byte("base")))
			must(t, d.Flush())
			must(t, d.Close())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashHelper$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "ATLAS_LSM_CRASH_CHILD=1", "ATLAS_LSM_CRASH_POINT="+point, "ATLAS_LSM_CRASH_DIR="+dir)
			pipe, e := cmd.StdoutPipe()
			must(t, e)
			cmd.Stderr = os.Stderr
			must(t, cmd.Start())
			ready := make(chan int, 1)
			go func() {
				ack := 0
				s := bufio.NewScanner(pipe)
				for s.Scan() {
					line := s.Text()
					if line == "READY" {
						ready <- ack
						return
					}
					if strings.HasPrefix(line, "ACK ") {
						ack, _ = strconv.Atoi(strings.TrimPrefix(line, "ACK "))
					}
				}
				ready <- -1
			}()
			ack := -1
			select {
			case ack = <-ready:
			case <-ctx.Done():
			}
			if ack < 0 {
				cmd.Process.Kill()
				cmd.Wait()
				t.Fatal("child did not reach boundary")
			}
			if second, e := Open(dir, Options{}); !errors.Is(e, ErrLocked) {
				if second != nil {
					second.Close()
				}
				cmd.Process.Kill()
				cmd.Wait()
				t.Fatal("process lock was bypassed", e)
			}
			must(t, cmd.Process.Kill())
			if e = cmd.Wait(); e == nil {
				t.Fatal("child exited without kill")
			}
			d = mustOpen(t, dir, Options{})
			defer d.Close()
			s, e := d.Stats()
			must(t, e)
			prefix := int(s.Sequence) - 3
			// Process death preserves the OS page cache. A completed write may
			// survive without an acknowledgment; acknowledged sync writes must.
			if prefix < ack || prefix > 3 {
				t.Fatal("acknowledged prefix lost", prefix, ack)
			}
			got, e := d.Scan(nil, nil, maxEntries)
			must(t, e)
			if !reflect.DeepEqual(got, sortedPairs(powerOracle(prefix))) {
				t.Fatal("recovered values differ", point, prefix)
			}
		})
	}
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	w := &smallWriter{}
	n, e := writeAll(w, []byte("abcdef"))
	if e != nil || n != 6 || string(w.b) != "abcdef" {
		t.Fatal(n, e, string(w.b))
	}
	if _, e = writeAll(stalledWriter{}, []byte("x")); !errors.Is(e, io.ErrShortWrite) {
		t.Fatal(e)
	}
}

type smallWriter struct{ b []byte }

func (w *smallWriter) Write(b []byte) (int, error) {
	if len(b) > 2 {
		b = b[:2]
	}
	w.b = append(w.b, b...)
	return len(b), nil
}

type stalledWriter struct{}

func (stalledWriter) Write([]byte) (int, error) { return 0, nil }
