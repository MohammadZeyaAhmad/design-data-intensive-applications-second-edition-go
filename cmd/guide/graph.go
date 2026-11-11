package main

import (
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
)

func graphPort(key, fallback string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		v = fallback
	}
	n, e := strconv.Atoi(v)
	if e != nil || n < 1024 || n > 65535 {
		return "", errors.New("invalid graph profile port")
	}
	return v, nil
}
func verifyGraphServices() error {
	if e := config.LoadLocalEnv(".env"); e != nil {
		return e
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	graphPassword := os.Getenv("ATLAS_GRAPH_PASSWORD")
	if password == "" || len(graphPassword) < 12 {
		return errors.New("configure the graph profile first")
	}
	pgPort, e := graphPort("ATLAS_GRAPH_POSTGRES_PORT", "17433")
	if e != nil {
		return e
	}
	boltPort, e := graphPort("ATLAS_GRAPH_BOLT_PORT", "17687")
	if e != nil {
		return e
	}
	u := url.URL{Scheme: "postgresql", User: url.UserPassword("atlas", password), Host: net.JoinHostPort("127.0.0.1", pgPort), Path: "/atlas_test", RawQuery: "sslmode=disable"}
	env := append(os.Environ(), "ATLAS_GRAPH_POSTGRES_URL="+u.String(), "ATLAS_GRAPH_NEO4J_URI=bolt://"+net.JoinHostPort("127.0.0.1", boltPort))
	for _, args := range [][]string{{"test", "-race", "-tags=graphintegration", "-count=1", "./ch03/property-graph/integration"}, {"run", "./cmd/lab", "graph-services", "-output", "../artifacts/graph-services.json"}} {
		cmd := exec.Command("go", args...)
		cmd.Dir = "labs"
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if e = cmd.Run(); e != nil {
			return e
		}
	}
	return nil
}
