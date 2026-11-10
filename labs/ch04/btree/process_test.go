package btree

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestProcessCrashHelper(t *testing.T) {
	if os.Getenv("ATLAS_BTREE_CHILD") != "1" {
		return
	}
	point := os.Getenv("ATLAS_BTREE_POINT")
	nth, e := strconv.Atoi(os.Getenv("ATLAS_BTREE_NTH"))
	must(t, e)
	hits := 0
	o := Options{Failpoint: func(p string) error {
		if p == point {
			hits++
			if hits == nth {
				fmt.Println("READY")
				for {
					time.Sleep(time.Hour)
				}
			}
		}
		return nil
	}}
	d := mustOpen(t, os.Getenv("ATLAS_BTREE_DIR"), o)
	defer d.Close()
	if os.Getenv("ATLAS_BTREE_RECOVER") != "1" {
		must(t, d.Put(key(4), []byte("new")))
	}
	t.Fatal("crash point not reached")
}
func killAt(t *testing.T, dir, point string, nth int, recovery bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashHelper$", "-test.timeout=15s")
	mode := "0"
	if recovery {
		mode = "1"
	}
	cmd.Env = append(os.Environ(), "ATLAS_BTREE_CHILD=1", "ATLAS_BTREE_POINT="+point, "ATLAS_BTREE_NTH="+strconv.Itoa(nth), "ATLAS_BTREE_DIR="+dir, "ATLAS_BTREE_RECOVER="+mode)
	pipe, e := cmd.StdoutPipe()
	must(t, e)
	cmd.Stderr = os.Stderr
	must(t, cmd.Start())
	ready := make(chan bool, 1)
	go func() {
		s := bufio.NewScanner(pipe)
		for s.Scan() {
			if s.Text() == "READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	var ok bool
	select {
	case ok = <-ready:
	case <-ctx.Done():
	}
	if !ok {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("child did not reach", point, nth)
	}
	if opened, e := Open(dir, Options{}); !errors.Is(e, ErrLocked) {
		if opened != nil {
			opened.Close()
		}
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("writer lock bypassed", e)
	}
	must(t, cmd.Process.Kill())
	if e = cmd.Wait(); e == nil {
		t.Fatal("child was not killed")
	}
}
func seedSplit(t *testing.T, dir string) {
	t.Helper()
	d := mustOpen(t, dir, Options{MaxKeys: 4})
	defer d.Close()
	for i := 0; i < 4; i++ {
		must(t, d.Put(key(i), []byte("base")))
	}
}
func splitMap(withNew bool) map[string]string {
	m := map[string]string{}
	for i := 0; i < 4; i++ {
		m[string(key(i))] = "base"
	}
	if withNew {
		m[string(key(4))] = "new"
	}
	return m
}

func TestRealProcessKillDuringSplitAndCheckpoint(t *testing.T) {
	cases := []struct {
		point string
		nth   int
	}{{"wal.written", 1}, {"wal.synced", 1}, {"checkpoint.page.written", 1}, {"checkpoint.page.written", 2}, {"checkpoint.page.written", 3}, {"checkpoint.data.resized", 1}, {"checkpoint.meta.written", 1}, {"checkpoint.data.synced", 1}, {"checkpoint.wal.truncated", 1}, {"checkpoint.wal.synced", 1}}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%d", c.point, c.nth), func(t *testing.T) {
			dir := t.TempDir()
			seedSplit(t, dir)
			killAt(t, dir, c.point, c.nth, false)
			d := mustOpen(t, dir, Options{})
			defer d.Close()
			s, e := d.Stats()
			must(t, e)
			if c.point == "wal.written" {
				if s.Generation != 4 && s.Generation != 5 {
					t.Fatal(s)
				}
			} else if s.Generation != 5 {
				t.Fatal("durable redo lost", s)
			}
			assertMap(t, d, splitMap(s.Generation == 5))
		})
	}
}
func TestRealProcessKillDuringRecovery(t *testing.T) {
	cases := []struct {
		point string
		nth   int
	}{{"recovery.page.written", 1}, {"recovery.page.written", 2}, {"recovery.page.written", 3}, {"recovery.data.resized", 1}, {"recovery.meta.written", 1}, {"recovery.data.synced", 1}, {"recovery.wal.truncated", 1}, {"recovery.wal.synced", 1}}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%d", c.point, c.nth), func(t *testing.T) {
			dir := t.TempDir()
			seedSplit(t, dir)
			killAt(t, dir, "wal.synced", 1, false)
			killAt(t, dir, c.point, c.nth, true)
			d := mustOpen(t, dir, Options{})
			defer d.Close()
			assertMap(t, d, splitMap(true))
			s, e := d.Stats()
			must(t, e)
			if s.Generation != 5 {
				t.Fatal(s)
			}
		})
	}
}
