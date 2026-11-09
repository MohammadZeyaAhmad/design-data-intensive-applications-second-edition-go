package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestDomainAndFeatureDependencyBoundaries(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "../..")
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, i := range f.Imports {
			imp, _ := strconv.Unquote(i.Path.Value)
			if strings.Contains(imp, "/labs/") {
				t.Errorf("production imports lab code: %s", path)
			}
			if strings.Contains(filepath.ToSlash(path), "/domain/") && strings.Contains(strings.Split(imp, "/")[0], ".") {
				t.Errorf("domain depends on infrastructure: %s imports %s", path, imp)
			}
			if strings.Contains(filepath.ToSlash(path), "/features/") && (strings.Contains(imp, "/adapters/") || strings.HasPrefix(imp, "github.com/jackc/")) {
				t.Errorf("feature imports concrete persistence: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
