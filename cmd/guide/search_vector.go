package main

import (
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
)

// The isolated vector test profile uses a separate port and named volume. Its
// connection string stays in the child environment, never process arguments.
func verifySearchVector() error {
	if e := config.LoadLocalEnv(".env"); e != nil {
		return e
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		return errors.New("configure the vector test profile first")
	}
	u := &url.URL{Scheme: "postgresql", User: url.UserPassword("atlas", password), Path: "/atlas_test", RawQuery: "sslmode=disable"}
	port := os.Getenv("ATLAS_SEARCH_VECTOR_TEST_PORT")
	if port == "" {
		port = "16433"
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 1024 || n > 65535 {
		return errors.New("invalid vector test port")
	}
	u.Host = net.JoinHostPort("127.0.0.1", port)
	report, e := filepath.Abs("artifacts/search-pgvector.json")
	if e != nil {
		return e
	}
	cmd := exec.Command("go", "test", "-race", "-tags=integration,searchvector", "-run", "^TestSearch", "-count=1", "./tests/integration")
	cmd.Env = append(os.Environ(), "ATLAS_TEST_DATABASE_URL="+u.String(), "ATLAS_SEARCH_VECTOR_REPORT="+report)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
