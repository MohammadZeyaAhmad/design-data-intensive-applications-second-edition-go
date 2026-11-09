package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func implementationProgress() error {
	var baseline struct {
		Packages []struct {
			ID string `json:"id"`
		} `json:"packages"`
	}
	b, err := os.ReadFile("docs/plan/work-packages.json")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &baseline); err != nil {
		return err
	}
	known := map[string]bool{}
	for _, p := range baseline.Packages {
		known[p.ID] = true
	}
	var current struct {
		Packages []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"packages"`
		Scenarios []string `json:"scenarios"`
	}
	b, err = os.ReadFile("docs/progress.json")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &current); err != nil {
		return err
	}
	seen := map[string]bool{}
	counts := map[string]int{}
	for _, p := range current.Packages {
		if !known[p.ID] || seen[p.ID] {
			return fmt.Errorf("invalid or repeated progress package %s", p.ID)
		}
		seen[p.ID] = true
		switch p.Status {
		case "planned", "in_progress", "implemented", "verified", "reviewed":
			counts[p.Status]++
		default:
			return fmt.Errorf("invalid progress status for %s", p.ID)
		}
	}
	if len(seen) != len(known) {
		return errors.New("implementation tracker omitted a planned package")
	}
	ids := map[string]bool{}
	verified := 0
	for _, path := range current.Scenarios {
		if err = safeEvidencePath(path); err != nil {
			return err
		}
		var scenario struct {
			ID       string   `json:"id"`
			Status   string   `json:"status"`
			Packages []string `json:"work_packages"`
			Evidence []string `json:"evidence"`
		}
		b, err = os.ReadFile(path)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(b, &scenario); err != nil {
			return err
		}
		if scenario.ID == "" || ids[scenario.ID] || len(scenario.Evidence) == 0 {
			return errors.New("scenario lacks unique identity or evidence")
		}
		ids[scenario.ID] = true
		for _, id := range scenario.Packages {
			if !known[id] {
				return fmt.Errorf("scenario refers to unknown package %s", id)
			}
		}
		for _, file := range scenario.Evidence {
			if err = safeEvidencePath(file); err != nil {
				return err
			}
			if _, err = os.Stat(file); err != nil {
				return err
			}
		}
		if scenario.Status == "verified" {
			verified++
		}
	}
	fmt.Printf("Implementation tracker: %d packages retained, %d in progress, %d planned; %d verified scenario manifests.\n", len(known), counts["in_progress"], counts["planned"], verified)
	fmt.Println("Manifest checks validate references; live verification commands provide behavioral evidence.")
	return nil
}
func safeEvidencePath(path string) error {
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("scenario evidence must be inside the repository")
	}
	return nil
}
