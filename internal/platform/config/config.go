package config

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
)

// LoadLocalEnv never overrides injected environment variables or prints values.
func LoadLocalEnv(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return errors.New("invalid local environment file")
		}
		if !strings.HasPrefix(key, "ATLAS_") && key != "POSTGRES_PASSWORD" {
			return errors.New("unsupported local environment variable")
		}
		if _, present := os.LookupEnv(key); !present {
			if err = os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return s.Err()
}

func TenantKeys(raw string) (map[[32]byte]string, error) {
	keys := map[[32]byte]string{}
	for _, entry := range strings.Split(raw, ",") {
		tenant, key, ok := strings.Cut(entry, "=")
		if !ok || !domain.ValidTenant(tenant) || len(key) < 32 || len(key) > 256 || strings.ContainsAny(key, " \t\r\n") {
			return nil, errors.New("ATLAS_TENANT_KEYS must contain tenant=key entries with keys of at least 32 characters")
		}
		hash := sha256.Sum256([]byte(key))
		if _, exists := keys[hash]; exists {
			return nil, fmt.Errorf("duplicate development authentication key")
		}
		keys[hash] = tenant
	}
	return keys, nil
}
