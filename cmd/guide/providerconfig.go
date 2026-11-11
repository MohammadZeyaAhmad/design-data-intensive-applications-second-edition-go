package main

import (
	"errors"
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
