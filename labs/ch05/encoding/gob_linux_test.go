//go:build linux

package encoding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

// Observe the actual production helper's kernel limits while its stdin remains
// open, then prove that the same constrained process decodes a normal record.
func TestGobWorkerKernelLimitsAndValidDecode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, testSuite.GobHelper)
	cmd.Env = append(os.Environ(), "GOMEMLIMIT=64MiB", "GOGC=50")
	stdin, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	defer stdin.Close()
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
	}()
	want := map[string]string{
		"Max cpu time":      "2                    2                    seconds",
		"Max data size":     "268435456            268435456            bytes",
		"Max address space": "4294967296           4294967296           bytes",
	}
	observed := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		limits, readError := os.ReadFile(fmt.Sprintf("/proc/%d/limits", cmd.Process.Pid))
		if readError != nil {
			t.Fatal(readError)
		}
		matched := 0
		for _, line := range strings.Split(string(limits), "\n") {
			for name, values := range want {
				if strings.HasPrefix(line, name) && strings.Join(strings.Fields(strings.TrimPrefix(line, name)), " ") == strings.Join(strings.Fields(values), " ") {
					matched++
				}
			}
		}
		if matched == len(want) {
			observed = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !observed {
		t.Fatal("worker did not install the expected kernel resource limits")
	}
	r := model.Fixture(100, 2)
	wire, e := testSuite.Encode("gob", 2, r)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stdin.Write(append([]byte{2}, wire...)); e != nil {
		t.Fatal(e)
	}
	stdin.Close()
	if e = cmd.Wait(); e != nil {
		t.Fatal(e)
	}
	var got model.Record
	if e = json.Unmarshal(stdout.Bytes(), &got); e != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("resource limits prevented valid decoding", e)
	}
}
