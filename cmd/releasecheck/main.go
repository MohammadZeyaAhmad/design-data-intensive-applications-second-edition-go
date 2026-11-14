// releasecheck creates and verifies release evidence from an actual Go binary.
// Module h1 directory hashes remain distinct from artifact file SHA-256 hashes.
package main

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go"

type manifest struct {
	Format       int    `json:"format"`
	Module       string `json:"module"`
	Revision     string `json:"revision"`
	Dirty        bool   `json:"dirty"`
	Go           string `json:"go_version"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	CGO          string `json:"cgo"`
	Dependencies int    `json:"dependencies"`
	BinarySHA    string `json:"binary_sha256"`
	SBOMSHA      string `json:"sbom_sha256"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "releasecheck:", err)
		os.Exit(1)
	}
}
func digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
func run() error {
	flags := flag.NewFlagSet("releasecheck", flag.ContinueOnError)
	binary := flags.String("binary", "bin/atlas", "Go executable")
	output := flags.String("output", "artifacts/release/linux-amd64", "artifact directory")
	expected := flags.String("revision", "", "required VCS revision")
	clean := flags.Bool("require-clean", false, "require clean VCS build settings")
	verify := flags.Bool("verify", false, "verify an existing bundle including binary build information")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected argument")
	}
	if *verify {
		return verifyBundle(*output)
	}
	info, err := buildinfo.ReadFile(*binary)
	if err != nil {
		return errors.New("input is not a readable Go executable")
	}
	if info.Main.Path != modulePath {
		return errors.New("unexpected main module")
	}
	settings := map[string]string{}
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["CGO_ENABLED"] != "0" || settings["GOOS"] != "linux" || (settings["GOARCH"] != "amd64" && settings["GOARCH"] != "arm64") {
		return errors.New("release requires a static Linux amd64/arm64 build")
	}
	if *expected != "" && settings["vcs.revision"] != *expected {
		return errors.New("binary does not match the required revision")
	}
	if *clean && (settings["vcs.modified"] != "false" || settings["vcs.revision"] == "") {
		return errors.New("release binary has no clean VCS provenance")
	}
	sums, err := os.ReadFile("go.sum")
	if err != nil {
		return err
	}
	known := map[string]string{}
	for _, line := range strings.Split(string(sums), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 {
			known[fields[0]+" "+fields[1]] = fields[2]
		}
	}
	type property struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type component struct {
		Type       string     `json:"type"`
		Name       string     `json:"name"`
		Version    string     `json:"version"`
		Purl       string     `json:"purl"`
		Properties []property `json:"properties,omitempty"`
	}
	components := make([]component, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		if dependency.Replace != nil || dependency.Sum == "" || known[dependency.Path+" "+dependency.Version] != dependency.Sum {
			return errors.New("binary dependency is replaced, unchecksummed or absent from go.sum")
		}
		parts := strings.Split(dependency.Path, "/")
		for i := range parts {
			parts[i] = url.PathEscape(parts[i])
		}
		components = append(components, component{Type: "library", Name: dependency.Path, Version: dependency.Version, Purl: "pkg:golang/" + strings.Join(parts, "/") + "@" + url.PathEscape(dependency.Version), Properties: []property{{Name: "go.module.sum", Value: dependency.Sum}}})
	}
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })
	if err = os.MkdirAll(*output, 0755); err != nil {
		return err
	}
	target := filepath.Join(*output, "atlas")
	sourceAbs, _ := filepath.Abs(*binary)
	targetAbs, _ := filepath.Abs(target)
	if sourceAbs != targetAbs {
		input, err := os.Open(*binary)
		if err != nil {
			return err
		}
		defer input.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, input)
		closeErr := out.Close()
		if err = errors.Join(copyErr, closeErr); err != nil {
			return err
		}
	}
	sbom := map[string]any{"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1, "metadata": map[string]any{"component": map[string]string{"type": "application", "name": "atlas", "version": settings["vcs.revision"]}}, "components": components}
	if err = writeJSON(filepath.Join(*output, "sbom.cdx.json"), sbom); err != nil {
		return err
	}
	binaryHash, err := digest(target)
	if err != nil {
		return err
	}
	sbomHash, err := digest(filepath.Join(*output, "sbom.cdx.json"))
	if err != nil {
		return err
	}
	m := manifest{Format: 1, Module: info.Main.Path, Revision: settings["vcs.revision"], Dirty: settings["vcs.modified"] != "false", Go: info.GoVersion, OS: settings["GOOS"], Arch: settings["GOARCH"], CGO: settings["CGO_ENABLED"], Dependencies: len(components), BinarySHA: binaryHash, SBOMSHA: sbomHash}
	if err = writeJSON(filepath.Join(*output, "manifest.json"), m); err != nil {
		return err
	}
	var lines []string
	for _, name := range []string{"atlas", "manifest.json", "sbom.cdx.json"} {
		hash, err := digest(filepath.Join(*output, name))
		if err != nil {
			return err
		}
		lines = append(lines, hash+"  "+name)
	}
	if err = os.WriteFile(filepath.Join(*output, "SHA256SUMS"), []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return err
	}
	if err = verifyBundle(*output); err != nil {
		return err
	}
	fmt.Printf("Release evidence: %s/%s, %d checked dependencies, revision %s, dirty=%t\n", m.OS, m.Arch, m.Dependencies, m.Revision, m.Dirty)

	return nil
}
func verifyBundle(directory string) error {
	data, err := os.ReadFile(filepath.Join(directory, "SHA256SUMS"))
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || seen[fields[1]] || (fields[1] != "atlas" && fields[1] != "manifest.json" && fields[1] != "sbom.cdx.json") {
			return errors.New("invalid release checksum inventory")
		}
		seen[fields[1]] = true
		actual, err := digest(filepath.Join(directory, fields[1]))
		if err != nil || actual != fields[0] {
			return errors.New("release artifact checksum mismatch")
		}
	}
	if len(seen) != 3 {
		return errors.New("incomplete release bundle")
	}
	var m manifest
	body, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return err
	}
	if json.Unmarshal(body, &m) != nil || m.Format != 1 || m.Module != modulePath {
		return errors.New("invalid release manifest")
	}
	info, err := buildinfo.ReadFile(filepath.Join(directory, "atlas"))
	if err != nil {
		return err
	}
	settings := map[string]string{}
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	hash, _ := digest(filepath.Join(directory, "atlas"))
	sbomHash, _ := digest(filepath.Join(directory, "sbom.cdx.json"))
	if info.Main.Path != m.Module || info.GoVersion != m.Go || settings["vcs.revision"] != m.Revision || settings["GOARCH"] != m.Arch || settings["CGO_ENABLED"] != "0" || settings["GOOS"] != m.OS || m.CGO != "0" || (settings["vcs.modified"] != "false") != m.Dirty || hash != m.BinarySHA || sbomHash != m.SBOMSHA || len(info.Deps) != m.Dependencies {
		return errors.New("manifest does not describe the actual executable")
	}
	var sbom struct {
		Format     string `json:"bomFormat"`
		Version    string `json:"specVersion"`
		Components []struct {
			Name       string `json:"name"`
			Version    string `json:"version"`
			Properties []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"properties"`
		} `json:"components"`
	}
	body, err = os.ReadFile(filepath.Join(directory, "sbom.cdx.json"))
	if err != nil {
		return err
	}
	if json.Unmarshal(body, &sbom) != nil || sbom.Format != "CycloneDX" || sbom.Version != "1.6" || len(sbom.Components) != m.Dependencies {
		return errors.New("invalid SBOM inventory")
	}
	if err := validateSBOM(body); err != nil {
		return err
	}
	actual := map[string]string{}
	for _, dependency := range info.Deps {
		actual[dependency.Path+" "+dependency.Version] = dependency.Sum
	}
	for _, component := range sbom.Components {
		sum := ""
		for _, property := range component.Properties {
			if property.Name == "go.module.sum" {
				sum = property.Value
			}
		}
		expected, ok := actual[component.Name+" "+component.Version]
		if !ok || sum != expected {
			return errors.New("SBOM does not describe the binary dependencies")
		}
		delete(actual, component.Name+" "+component.Version)
	}
	if len(actual) != 0 {
		return errors.New("SBOM omits binary dependencies")
	}
	return nil
}
