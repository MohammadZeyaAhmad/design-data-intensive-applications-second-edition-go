package objectstorage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskio"
)

type FileStore struct{ root string }
type fileObject struct {
	Key     string `json:"key"`
	Version string `json:"version"`
	Body    []byte `json:"body"`
}

func NewFileStore(root string) (*FileStore, error) {
	if !filepath.IsAbs(root) {
		return nil, ErrInvalid
	}
	fs := diskio.OS{}
	if err := fs.MkdirAll(root); err != nil {
		return nil, err
	}
	lock, err := lockFile(context.Background(), filepath.Join(root, ".operations.lock"))
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	marker := filepath.Join(root, ".atlas-object-store-v1")
	b, err := os.ReadFile(marker)
	if os.IsNotExist(err) {
		entries, e := os.ReadDir(root)
		if e != nil {
			return nil, e
		}
		if len(entries) != 1 || entries[0].Name() != ".operations.lock" {
			return nil, ErrInvalid
		}
		f, e := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		_, e = f.WriteString("atlas-object-store-v1\n")
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			return nil, e
		}
	} else if err != nil {
		return nil, err
	} else if string(b) != "atlas-object-store-v1\n" {
		return nil, ErrInvalid
	}
	if err = fs.MkdirAll(filepath.Join(root, "objects")); err != nil {
		return nil, err
	}
	if err = fs.SyncDir(root); err != nil {
		return nil, err
	}
	return &FileStore{root: root}, nil
}
func (s *FileStore) path(key string) string {
	return filepath.Join(s.root, "objects", sum([]byte(key))+".json")
}
func (s *FileStore) get(key string) (fileObject, error) {
	path := s.path(key)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return fileObject{}, ErrNotFound
	}
	if err != nil {
		return fileObject{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > 2*MaxObjectBytes {
		return fileObject{}, ErrCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return fileObject{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 2*MaxObjectBytes+1))
	if err != nil {
		return fileObject{}, err
	}
	var o fileObject
	if strictJSON(b, &o) != nil || o.Key != key || len(o.Body) > MaxObjectBytes || o.Version == "" {
		return fileObject{}, ErrCorrupt
	}
	return o, nil
}
func (s *FileStore) Get(ctx context.Context, key string) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if !validKey(key) {
		return Object{}, ErrInvalid
	}
	o, err := s.get(key)
	return Object{o.Body, o.Version}, err
}
func (s *FileStore) Put(ctx context.Context, key string, body []byte, c Condition) (string, error) {
	if !validKey(key) || !c.valid() || len(body) > MaxObjectBytes {
		return "", ErrInvalid
	}
	lock, err := lockFile(ctx, filepath.Join(s.root, ".operations.lock"))
	if err != nil {
		return "", err
	}
	defer lock.Close()
	old, err := s.get(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return "", err
	}
	exists := err == nil
	if (c.Absent && exists) || (!c.Absent && (!exists || old.Version != c.Version)) {
		return "", ErrConflict
	}
	if !exists {
		es, e := os.ReadDir(filepath.Join(s.root, "objects"))
		if e != nil {
			return "", e
		}
		n := 0
		for _, entry := range es {
			if strings.HasSuffix(entry.Name(), ".json") {
				n++
			}
		}
		if n >= MaxObjects {
			return "", ErrLimit
		}
	}
	token := make([]byte, 24)
	if _, err = rand.Read(token); err != nil {
		return "", err
	}
	version := `"file-` + hex.EncodeToString(token) + `"`
	b, err := json.Marshal(fileObject{key, version, body})
	if err != nil {
		return "", err
	}
	if err = atomicFile(s.path(key), b); err != nil {
		return "", errors.Join(ErrUncertain, err)
	}
	return version, nil
}
func atomicFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".object-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return (diskio.OS{}).SyncDir(filepath.Dir(path))
}
func (s *FileStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if len(keys) >= MaxObjects {
			return nil, ErrLimit
		}
		info, e := entry.Info()
		if e != nil {
			return nil, e
		}
		if !info.Mode().IsRegular() || info.Size() > 2*MaxObjectBytes {
			return nil, ErrCorrupt
		}
		b, e := os.ReadFile(filepath.Join(s.root, "objects", entry.Name()))
		if e != nil {
			return nil, e
		}
		var o fileObject
		if len(b) > 2*MaxObjectBytes || strictJSON(b, &o) != nil || !validKey(o.Key) || s.path(o.Key) != filepath.Join(s.root, "objects", entry.Name()) {
			return nil, ErrCorrupt
		}
		if strings.HasPrefix(o.Key, prefix) {
			keys = append(keys, o.Key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
func (s *FileStore) Delete(ctx context.Context, key string) error {
	if !validKey(key) {
		return ErrInvalid
	}
	lock, err := lockFile(ctx, filepath.Join(s.root, ".operations.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	err = os.Remove(s.path(key))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return (diskio.OS{}).SyncDir(filepath.Join(s.root, "objects"))
}
