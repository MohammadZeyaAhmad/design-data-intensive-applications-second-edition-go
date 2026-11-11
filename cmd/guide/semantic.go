package main

import (
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"net"
	"net/url"
	"os"
	"os/exec"
)

func verifySemanticServices() error {
	if e := config.LoadLocalEnv(".env"); e != nil {
		return e
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" || len(os.Getenv("ATLAS_RDF_PASSWORD")) < 12 {
		return errors.New("configure the semantic profile first")
	}
	pgPort, e := graphPort("ATLAS_RDF_POSTGRES_PORT", "18433")
	if e != nil {
		return e
	}
	rdfPort, e := graphPort("ATLAS_RDF_HTTP_PORT", "18303")
	if e != nil {
		return e
	}
	u := url.URL{Scheme: "postgresql", User: url.UserPassword("atlas", password), Host: net.JoinHostPort("127.0.0.1", pgPort), Path: "/atlas_test", RawQuery: "sslmode=disable"}
	env := append(os.Environ(), "ATLAS_RDF_POSTGRES_URL="+u.String(), "ATLAS_RDF_URL=http://"+net.JoinHostPort("127.0.0.1", rdfPort)+"/routes")
	for _, args := range [][]string{{"test", "-race", "-tags=semanticintegration", "-count=1", "./ch03/semantic-graph/integration"}, {"run", "./cmd/lab", "semantic-services", "-output", "../artifacts/semantic-services.json"}} {
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
