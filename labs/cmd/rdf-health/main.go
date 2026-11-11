package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, e := http.NewRequestWithContext(ctx, "POST", os.Getenv("ATLAS_RDF_URL")+"/query", bytes.NewBufferString("ASK {}"))
	if e != nil {
		os.Exit(1)
	}
	request.SetBasicAuth("atlas", os.Getenv("ATLAS_RDF_PASSWORD"))
	request.Header.Set("Content-Type", "application/sparql-query")
	request.Header.Set("Accept", "application/sparql-results+json")
	client := http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, e := client.Do(request)
	if e != nil {
		os.Exit(1)
	}
	defer response.Body.Close()
	var result struct {
		Boolean bool `json:"boolean"`
	}
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil || !result.Boolean {
		os.Exit(1)
	}
}
