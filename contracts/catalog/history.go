package catalog

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"unicode/utf8"
)

//go:embed historyschema/*.json
var historySchemas embed.FS
var ErrHistorySchema = errors.New("unsupported or invalid brand history contract")
var historyID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type BrandSnapshot struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Version     int64  `json:"version"`
}

func BrandHistorySchema(version int) ([]byte, error) {
	if version != 1 && version != 2 {
		return nil, ErrHistorySchema
	}
	return historySchemas.ReadFile("historyschema/v" + strconv.Itoa(version) + ".json")
}
func HistoryDigest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

// UpcastBrandHistory transforms an explicitly versioned source into the current
// contract in memory. It never writes source bytes or infers schema from Go.
func UpcastBrandHistory(version int, raw []byte) (BrandSnapshot, error) {
	var result BrandSnapshot
	if len(raw) < 2 || len(raw) > 4096 || !utf8.Valid(raw) {
		return result, ErrHistorySchema
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, e := d.Token()
	if e != nil || token != json.Delim('{') {
		return result, ErrHistorySchema
	}
	keys := map[string]bool{}
	for d.More() {
		token, e = d.Token()
		if e != nil {
			return result, ErrHistorySchema
		}
		key, ok := token.(string)
		if !ok || keys[key] || len(keys) >= 4 {
			return result, ErrHistorySchema
		}
		keys[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return result, ErrHistorySchema
		}
	}
	if token, e = d.Token(); e != nil || token != json.Delim('}') {
		return result, ErrHistorySchema
	}
	if _, e = d.Token(); e != io.EOF {
		return result, ErrHistorySchema
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	switch version {
	case 1:
		var old struct {
			ID      string `json:"id"`
			Name    string `json:"name"`
			Version int64  `json:"version"`
		}
		if d.Decode(&old) != nil {
			return result, ErrHistorySchema
		}
		result = BrandSnapshot{old.ID, old.Name, old.Version}
	case 2:
		if d.Decode(&result) != nil {
			return BrandSnapshot{}, ErrHistorySchema
		}
	default:
		return BrandSnapshot{}, ErrHistorySchema
	}
	if !historyID.MatchString(result.ID) || len(result.DisplayName) < 1 || len(result.DisplayName) > 200 || !utf8.ValidString(result.DisplayName) || bytes.ContainsRune([]byte(result.DisplayName), 0) || result.Version < 1 || result.Version >= 1e12 {
		return BrandSnapshot{}, ErrHistorySchema
	}
	return result, nil
}
