package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func providerConfig() error {
	lock, err := os.OpenFile(".env.configure.lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("local configuration is already being updated")
	}
	lock.Close()
	defer os.Remove(".env.configure.lock")
	info, err := os.Stat(".env")
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0077 != 0 {
		return errors.New(".env permissions must restrict access to its owner")
	}
	b, err := os.ReadFile(".env")
	if err != nil {
		return err
	}
	text := string(b)
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_PROVIDER_TOKEN=") {
		token, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_PROVIDER_TOKEN=" + token + "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_PROVIDER_URL=") {
		text += "ATLAS_PROVIDER_URL=http://127.0.0.1:18082\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_GRAPH_PASSWORD=") {
		password, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_GRAPH_PASSWORD=" + password + "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_RDF_PASSWORD=") {
		password, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_RDF_PASSWORD=" + password + "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_GRAPHQL_CURSOR_KEY=") {
		key, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_GRAPHQL_CURSOR_KEY=" + key + "\n"
	}

	if !strings.Contains("\n"+text, "\nATLAS_GRAFANA_PASSWORD=") {
		value, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_GRAFANA_PASSWORD=" + value + "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_TELEMETRY_TOKEN=") {
		value, err := secret()
		if err != nil {
			return err
		}
		text += "ATLAS_TELEMETRY_TOKEN=" + value + "\n"
	}
	if !strings.Contains("\n"+text, "\nATLAS_LOCAL_UID=") {
		text += fmt.Sprintf("ATLAS_LOCAL_UID=%d\nATLAS_LOCAL_GID=%d\n", os.Getuid(), os.Getgid())
	}
	token := os.Getenv("ATLAS_TELEMETRY_TOKEN")
	if token == "" {
		for _, line := range strings.Split(text, "\n") {
			if value, ok := strings.CutPrefix(line, "ATLAS_TELEMETRY_TOKEN="); ok {
				token = value
			}
		}
	}
	if len(token) < 32 {
		return errors.New("invalid local telemetry reader credential")
	}
	if err = os.WriteFile(".env.telemetry-token.next", []byte(token), 0600); err != nil {
		return err
	}
	if err = os.Rename(".env.telemetry-token.next", ".env.telemetry-token"); err != nil {
		return err
	}
	if text == string(b) {
		return nil
	}
	if err = os.WriteFile(".env.next", []byte(text), 0600); err != nil {
		return err
	}
	if err = os.Rename(".env.next", ".env"); err != nil {
		return err
	}
	return nil
}
