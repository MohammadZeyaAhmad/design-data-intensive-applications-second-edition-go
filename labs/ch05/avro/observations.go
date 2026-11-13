package avrolab

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"

	g2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/avro/generated/v2"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

type Observation struct {
	ID       string `json:"id"`
	Verified bool   `json:"verified"`
	Detail   string `json:"detail"`
}

func observations(ctx context.Context, r *Registry) ([]Observation, error) {
	var result []Observation
	add := func(id string, verified bool, detail string) {
		result = append(result, Observation{id, verified, detail})
	}
	old, _ := r.byVersion("v1")
	current, _ := r.byVersion("v2")
	newest, _ := r.byVersion("v3")
	input := fixture(1, "v1")
	wire, e := r.Encode(ctx, "v1", "generated", input)
	if e != nil {
		return nil, e
	}
	missing := NewRegistry()
	if _, e = missing.Register("v3", newest.full); e != nil {
		return nil, e
	}
	_, e = missing.Decode(ctx, old.info.SHA256, "v3", "dynamic", wire)
	add("missing-writer", errors.Is(e, ErrMissingSchema), "A current reader alone cannot decode a raw record without its exact writer schema; no latest-writer fallback exists.")
	var wrong any
	rest, wrongError := newest.schema.Decode(wire, &wrong)
	wrongRecord, recordError := recordFromMap("v3", wrong)
	add("field-order-needs-writer", wrongError != nil || len(rest) > 0 || recordError != nil || !reflect.DeepEqual(model.Known(wrongRecord), model.Known(input)), "Reading old bytes directly with a reordered latest schema fails or changes meaning; resolution first follows the writer's layout.")
	usd, e := r.Decode(ctx, old.info.SHA256, "v2", "generated", wire)
	if e != nil {
		return nil, e
	}
	eur, e := r.Decode(ctx, old.info.SHA256, "v3", "dynamic", wire)
	if e != nil {
		return nil, e
	}
	add("reader-defaults", usd.Currency != nil && *usd.Currency == "USD" && eur.Currency != nil && *eur.Currency == "EUR", "The same old bytes receive the chosen reader's USD or EUR currency default.")
	changed := bytes.Replace(current.full, []byte(`"default":"USD"`), []byte(`"default":"EUR"`), 1)
	other, e := entry("v2_eur", changed)
	if e != nil {
		return nil, e
	}
	add("canonical-versus-semantic-identity", current.info.Rabin == other.info.Rabin && current.info.CanonicalSHA256 == other.info.CanonicalSHA256 && current.info.SHA256 != other.info.SHA256, "Parsing Canonical Form strips defaults/aliases; a full-schema SHA-256 distinguishes semantic contracts sharing a fingerprint.")
	probeRegistry := NewRegistry()
	probeRegistry.Register("v2", current.full)
	probeRegistry.Register("v2_eur", changed)
	message, e := r.EncodeSingleObject(ctx, "v2", "generated", fixture(1, "v2"))
	if e != nil {
		return nil, e
	}
	_, e = probeRegistry.DecodeSingleObject(ctx, "v2", "dynamic", message)
	add("ambiguous-fingerprint", errors.Is(e, ErrAmbiguous), "CRC lookup rejects ambiguity instead of selecting an arbitrary full semantic version.")
	inferred, e := avro.SchemaFor[g2.OrderSnapshot](avro.WithNamespace("atlas.encoding"))
	if e != nil {
		return nil, e
	}
	inferredDefaults := map[string]bool{}
	for _, field := range inferred.Root().Fields {
		inferredDefaults[field.Name] = field.HasDefault
	}
	var compatibilityError *avro.CompatibilityError
	missingDefaults := errors.As(avro.CheckCompatibility(old.schema, inferred), &compatibilityError) && strings.Contains(compatibilityError.Detail, "default")
	add("generated-types-not-schema-history", missingDefaults && !inferredDefaults["currency"] && !inferredDefaults["labels"] && avro.CheckCompatibility(old.schema, current.schema) == nil, "Inferring a schema from generated field types does not reconstruct the authored currency/label defaults; the .avsc remains authoritative.")
	input = fixture(1, "v2")
	input.Expedited = nil
	input.Discount = nil
	wire, e = r.Encode(ctx, "v2", "generated", input)
	if e != nil {
		return nil, e
	}
	out, e := r.Decode(ctx, current.info.SHA256, "v3", "generated", wire)
	if e != nil {
		return nil, e
	}
	add("written-null-is-not-missing", out.Expedited == nil && out.Discount == nil, "A reader default does not replace a null branch actually written by the producer.")
	input = fixture(1, "v2")
	wire, e = r.Encode(ctx, "v2", "generated", input)
	if e != nil {
		return nil, e
	}
	out, e = r.Decode(ctx, current.info.SHA256, "v3", "dynamic", wire)
	if e != nil {
		return nil, e
	}
	add("union-false-zero-presence", out.Expedited != nil && !*out.Expedited && out.Discount != nil && *out.Discount == 0, "Reordered nullable unions remap branch indices and retain explicit false/zero values.")
	currency := "USD"
	input.Currency = &currency
	wire, e = r.Encode(ctx, "v2", "generated", input)
	if e != nil {
		return nil, e
	}
	add("default-value-still-on-wire", bytes.Contains(wire, []byte("USD")), "A default annotation does not remove a field from the writer's binary layout; this writer emits the USD value explicitly.")
	for _, observation := range result {
		if !observation.Verified {
			return nil, errors.New("Avro observation failed: " + observation.ID)
		}
	}
	if avroprofile.Scan(current.plan, wire) != nil {
		return nil, avroprofile.ErrInvalid
	}
	return result, nil
}
