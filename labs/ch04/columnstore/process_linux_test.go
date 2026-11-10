package columnstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestColumnPublicationChild(t *testing.T) {
	if os.Getenv("ATLAS_COLUMN_CHILD") != "1" {
		return
	}
	stage := os.Getenv("ATLAS_COLUMN_STAGE")
	d, e := Open(os.Getenv("ATLAS_COLUMN_PATH"), Options{Failpoint: func(p string) error {
		if p == stage {
			if e := syscall.Kill(os.Getpid(), syscall.SIGKILL); e != nil {
				t.Fatal(e)
			}
		}
		return nil
	}})
	must(t, e)
	switch strings.Split(stage, ".")[0] {
	case "ingest":
		r := fixture(1)
		r.Version = 3
		n := int64(777)
		r.Units = &n
		e = d.Put(r)
	case "flush":
		e = d.Flush()
	case "compact":
		e = d.Compact()
	case "cube":
		e = d.RebuildCube()
	}
	must(t, e)
	d.Close()
	t.Fatal("target boundary was not reached")
}
func TestNativeKillAtAtomicPublicationBoundaries(t *testing.T) {
	for _, operation := range []string{"ingest", "flush", "compact", "cube"} {
		for _, boundary := range []string{"ready", "committed"} {
			stage := operation + "." + boundary
			t.Run(stage, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "facts.bolt")
				d, e := Open(path, Options{})
				must(t, e)
				for i := 0; i < 32; i++ {
					must(t, d.Put(fixture(i)))
				}
				must(t, d.Flush())
				r := fixture(1)
				r.Version = 2
				must(t, d.Put(r))
				before, e := d.Stats()
				must(t, e)
				must(t, d.Close())
				cmd := exec.Command(os.Args[0], "-test.run=^TestColumnPublicationChild$")
				cmd.Env = append(os.Environ(), "ATLAS_COLUMN_CHILD=1", "ATLAS_COLUMN_PATH="+path, "ATLAS_COLUMN_STAGE="+stage)
				e = cmd.Run()
				exit, ok := e.(*exec.ExitError)
				if !ok {
					t.Fatal("child did not die", e)
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || status.Signal() != syscall.SIGKILL {
					t.Fatal("unexpected child exit", e)
				}
				d, e = Open(path, Options{})
				must(t, e)
				defer d.Close()
				after, e := d.Stats()
				must(t, e)
				got, e := d.Get(r.Tenant, r.ID)
				must(t, e)
				wantPublication, wantState, wantVersion := before.Publication, before.StateVersion, uint64(2)
				if boundary == "committed" {
					wantPublication++
					if operation == "ingest" {
						wantState++
						wantVersion = 3
					}
				}
				if after.Publication != wantPublication || after.StateVersion != wantState || got.Version != wantVersion {
					t.Fatal("partial or lost publication", after, got.Version)
				}
				must(t, d.Check())
			})
		}
	}
}
