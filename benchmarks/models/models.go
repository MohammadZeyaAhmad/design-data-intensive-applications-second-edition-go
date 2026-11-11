// Package modelbench runs real PostgreSQL representations and actual sqlc/GORM
// mappings over fresh tenant fixtures. Temporary comparison tables belong only
// to the dedicated connection; application pricing remains authoritative.
package modelbench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	catalogdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/describe"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errMismatch = errors.New("catalog model results differ")
var errStatements = errors.New("unexpected ORM association query count")

type Card struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Brand    string `json:"brand"`
	Price    int64  `json:"price"`
	Variants int    `json:"variants"`
}
type Sample struct {
	Mode       string `json:"mode"`
	Query      int    `json:"query"`
	Repetition int    `json:"repetition"`
	Rows       int    `json:"rows"`
	Digest     string `json:"digest"`
	WallNanos  int64  `json:"wall_nanos"`
}
type Plan struct {
	Mode    string          `json:"mode"`
	Indexed bool            `json:"indexed"`
	JSON    json.RawMessage `json:"explain_analyze_buffers"`
}
type NestedUpdate struct {
	Verified       bool            `json:"values_and_source_verified"`
	DocumentBefore int64           `json:"jsonb_relation_before_bytes"`
	DocumentAfter  int64           `json:"jsonb_relation_after_bytes"`
	DocumentWAL    json.RawMessage `json:"document_update_plan_wal"`
	RowWAL         json.RawMessage `json:"variant_row_update_plan_wal"`
}
type Report struct {
	Passed      bool         `json:"passed"`
	Rows        int          `json:"fixture_rows"`
	Repetitions int          `json:"repetitions"`
	Postgres    string       `json:"postgres_version"`
	Samples     []Sample     `json:"samples"`
	Plans       []Plan       `json:"plans"`
	Mappings    []Mapping    `json:"mappings"`
	Anomaly     bool         `json:"partial_rename_detected"`
	Repair      bool         `json:"repair_verified"`
	Nested      NestedUpdate `json:"nested_update"`
	Schema      bool         `json:"jsonb_schema_drift_detected"`
	Definitions []string     `json:"definitions"`
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	var canonical any
	_ = json.Unmarshal(b, &canonical)
	b, _ = json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type fixtureRow struct {
	Tenant, SKU, Name, Brand, Category string
	Price                              int64
	Variants                           int
}

const normalized = `SELECT p.sku,p.name,b.name,p.unit_price_minor,(SELECT count(*)::int FROM catalog.product_variants v WHERE v.tenant_id=h.tenant_id AND v.sku=h.sku) FROM catalog.products h JOIN catalog.prices p ON p.tenant_id=h.tenant_id AND p.sku=h.sku AND p.version=h.version JOIN catalog.product_details d ON d.tenant_id=h.tenant_id AND d.sku=h.sku JOIN catalog.brands b ON b.tenant_id=d.tenant_id AND b.id=d.brand_id WHERE h.tenant_id=$1 AND EXISTS(SELECT 1 FROM catalog.product_categories c WHERE c.tenant_id=h.tenant_id AND c.sku=h.sku AND c.category=$2) AND p.unit_price_minor<=$3 AND ($4='' OR EXISTS(SELECT 1 FROM model_favorites f WHERE f.tenant_id=h.tenant_id AND f.sku=h.sku AND f.customer=$4)) ORDER BY p.unit_price_minor,p.sku LIMIT $5`
const flatQuery = `SELECT sku,name,brand,price,count(DISTINCT variant)::int FROM model_flat WHERE tenant_id=$1 AND category=$2 AND price<=$3 AND ($4='' OR EXISTS(SELECT 1 FROM model_favorites f WHERE f.tenant_id=model_flat.tenant_id AND f.sku=model_flat.sku AND f.customer=$4)) GROUP BY sku,name,brand,price ORDER BY price,sku LIMIT $5`
const documentQuery = `SELECT sku,doc->>'name',doc->>'brand',(doc->>'price')::bigint,jsonb_array_length(doc->'variants') FROM model_documents WHERE tenant_id=$1 AND doc @> jsonb_build_object('categories',jsonb_build_array($2::text)) AND (doc->>'price')::bigint<=$3 AND ($4='' OR EXISTS(SELECT 1 FROM model_customers c WHERE c.tenant_id=model_documents.tenant_id AND c.id=$4 AND c.body->'favorites' ? model_documents.sku)) ORDER BY (doc->>'price')::bigint,sku LIMIT $5`
const wideQuery = `SELECT sku,identity->>'name',identity->>'brand',price,jsonb_array_length(options) FROM model_families WHERE tenant_id=$1 AND category=$2 AND price<=$3 AND ($4='' OR EXISTS(SELECT 1 FROM model_favorites f WHERE f.tenant_id=model_families.tenant_id AND f.sku=model_families.sku AND f.customer=$4)) ORDER BY price,sku LIMIT $5`

func query(ctx context.Context, c *pgx.Conn, sql string, args ...any) ([]Card, error) {
	rows, e := c.Query(ctx, sql, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Card{}
	for rows.Next() {
		var x Card
		if e = rows.Scan(&x.SKU, &x.Name, &x.Brand, &x.Price, &x.Variants); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func refresh(ctx context.Context, c *pgx.Conn, tenant string) error {
	_, e := c.Exec(ctx, `TRUNCATE model_flat,model_documents,model_families;
INSERT INTO model_documents SELECT h.tenant_id,h.sku,jsonb_build_object('schema',2,'sku',h.sku,'name',p.name,'brand',b.name,'price',p.unit_price_minor,'attributes',d.attributes,'categories',ARRAY(SELECT category FROM catalog.product_categories x WHERE x.tenant_id=h.tenant_id AND x.sku=h.sku ORDER BY category),'variants',(SELECT jsonb_agg(jsonb_build_object('code',v.code,'label',v.label) ORDER BY v.position) FROM catalog.product_variants v WHERE v.tenant_id=h.tenant_id AND v.sku=h.sku)) FROM catalog.products h JOIN catalog.prices p ON p.tenant_id=h.tenant_id AND p.sku=h.sku AND p.version=h.version JOIN catalog.product_details d ON d.tenant_id=h.tenant_id AND d.sku=h.sku JOIN catalog.brands b ON b.tenant_id=d.tenant_id AND b.id=d.brand_id WHERE h.tenant_id LIKE $1;
INSERT INTO model_flat SELECT d.tenant_id,d.sku,d.doc->>'name',d.doc->>'brand',(d.doc->>'price')::bigint,c.category,v.value->>'code' FROM model_documents d CROSS JOIN LATERAL jsonb_array_elements_text(d.doc->'categories') c(category) CROSS JOIN LATERAL jsonb_array_elements(d.doc->'variants') v;
INSERT INTO model_families SELECT d.tenant_id,c.category,(d.doc->>'price')::bigint,d.sku,jsonb_build_object('name',d.doc->>'name','brand',d.doc->>'brand'),d.doc->'variants' FROM model_documents d CROSS JOIN LATERAL jsonb_array_elements_text(d.doc->'categories') c(category);`, pgx.QueryExecModeSimpleProtocol, tenant+"%")
	return e
}
func Run(ctx context.Context, pool *pgxpool.Pool, rows, runs int) (Report, error) {
	r := Report{Rows: rows, Repetitions: runs}
	if rows < 32 || rows > 4096 || runs < 1 || runs > 5 {
		return r, domain.ErrInvalid
	}
	id, e := identity.NewID()
	if e != nil {
		return r, e
	}
	base := "model-" + strings.ReplaceAll(id, "-", "")
	a, b := base+"-acme", base+"-globex"
	s := &catalogdb.Store{Pool: pool}
	var fixture []fixtureRow
	var skus []string
	for _, tenant := range []string{a, b} {
		for i := 0; i < 8; i++ {
			_, _, e := (describe.Handler{Store: s}).Brand(ctx, tenant, fmt.Sprintf("brand-%d", i), domain.Brand{ID: fmt.Sprintf("brand-%d", i), Name: fmt.Sprintf("Maker %d", i)})
			if e != nil {
				return r, e
			}
		}
	}
	for i := 0; i < rows; i++ {
		tenant := a
		if i%2 != 0 {
			tenant = b
		}
		sku := fmt.Sprintf("sku-%05d", i)
		price := int64(100 + i*17%10000)
		name := fmt.Sprintf("Device %05d", i)
		if _, _, e = (publish.Handler{Store: s}).Handle(ctx, tenant, "price-"+sku, domain.Product{SKU: sku, Name: name, UnitPriceMinor: price, Currency: "USD"}); e != nil {
			return r, e
		}
		d := domain.Details{SKU: sku, BrandID: fmt.Sprintf("brand-%d", i%8), Description: "Model comparison fixture", Attributes: json.RawMessage(`{"material":"recycled","weight_grams":120}`), Categories: []string{"all", fmt.Sprintf("category-%d", i%4)}}
		for j := 0; j < 1+i%3; j++ {
			d.Variants = append(d.Variants, domain.Variant{Code: fmt.Sprintf("v-%d", j), Label: fmt.Sprintf("Option %d", j)})
		}
		if _, _, e = (describe.Handler{Store: s}).Handle(ctx, tenant, "details-"+sku, d); e != nil {
			return r, e
		}
		fixture = append(fixture, fixtureRow{tenant, sku, name, fmt.Sprintf("Maker %d", i%8), fmt.Sprintf("category-%d", i%4), price, len(d.Variants)})
		if tenant == a && len(skus) < 12 {
			skus = append(skus, sku)
		}
	}
	if r.Mappings, e = mapping(ctx, pool, a, skus); e != nil {
		return r, e
	}
	c, e := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if e != nil {
		return r, e
	}
	defer c.Close(context.Background())
	if e = c.QueryRow(ctx, "SHOW server_version").Scan(&r.Postgres); e != nil {
		return r, e
	}
	_, e = c.Exec(ctx, `CREATE TEMP TABLE model_documents(tenant_id text,sku text,doc jsonb);CREATE TEMP TABLE model_flat(tenant_id text,sku text,name text,brand text,price bigint,category text,variant text);CREATE TEMP TABLE model_families(tenant_id text,category text,price bigint,sku text,identity jsonb,options jsonb);CREATE TEMP TABLE model_favorites(tenant_id text,customer text,sku text,PRIMARY KEY(tenant_id,customer,sku));CREATE TEMP TABLE model_customers(tenant_id text,id text,body jsonb,PRIMARY KEY(tenant_id,id));`)
	if e != nil {
		return r, e
	}
	for _, tenant := range []string{a, b} {
		var favorites []string
		for i, p := range fixture {
			if p.Tenant == tenant && i%8 == 0 || p.Tenant == tenant && tenant == b && i%8 == 1 {
				favorites = append(favorites, p.SKU)
				if _, e = c.Exec(ctx, `INSERT INTO model_favorites VALUES($1,'customer-0',$2)`, tenant, p.SKU); e != nil {
					return r, e
				}
			}
		}
		body, _ := json.Marshal(map[string]any{"name": "Reader", "favorites": favorites})
		if _, e = c.Exec(ctx, `INSERT INTO model_customers VALUES($1,'customer-0',$2)`, tenant, body); e != nil {
			return r, e
		}
	}
	if e = refresh(ctx, c, base); e != nil {
		return r, e
	}
	// Keep statistics current in both phases so index addition is not confounded
	// with the first ANALYZE of a newly populated temporary relation.
	if _, e = c.Exec(ctx, `ANALYZE model_flat;ANALYZE model_documents;ANALYZE model_families;ANALYZE model_favorites;ANALYZE model_customers;ANALYZE catalog.products;ANALYZE catalog.prices;ANALYZE catalog.brands;ANALYZE catalog.product_details;ANALYZE catalog.product_categories;ANALYZE catalog.product_variants;`); e != nil {
		return r, e
	}
	cases := []struct{ mode, sql string }{{"normalized", normalized}, {"denormalized", flatQuery}, {"document", documentQuery}, {"wide-column", wideQuery}}
	args := []any{a, "category-0", int64(2500), "", 100}
	for _, indexed := range []bool{false, true} {
		if indexed {
			_, e = c.Exec(ctx, `CREATE INDEX model_flat_lookup ON model_flat(tenant_id,category,price,sku);CREATE INDEX model_document_contains ON model_documents USING gin(doc jsonb_path_ops);CREATE INDEX model_document_price ON model_documents(tenant_id,((doc->>'price')::bigint),sku);CREATE UNIQUE INDEX model_family_partition ON model_families(tenant_id,category,price,sku);ANALYZE model_flat;ANALYZE model_documents;ANALYZE model_families;ANALYZE catalog.product_details;ANALYZE catalog.product_categories;ANALYZE catalog.product_variants;`)
			if e != nil {
				return r, e
			}
		}
		for _, cs := range cases {
			var plan []byte
			if e = c.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+cs.sql, args...).Scan(&plan); e != nil {
				return r, e
			}
			r.Plans = append(r.Plans, Plan{cs.mode, indexed, plan})
		}
	}
	questions := [][]any{{a, "all", int64(1e12), "", 100}, {b, "category-1", int64(3000), "", 100}, {a, "all", int64(1e12), "customer-0", 100}, {"missing", "all", int64(1e12), "", 100}}
	for qi, qa := range questions {
		want := []Card{}
		for i, p := range fixture {
			if p.Tenant != qa[0] || p.Price > qa[2].(int64) || (qa[1] != "all" && p.Category != qa[1]) || (qa[3] != "" && i%8 != 0) {
				continue
			}
			want = append(want, Card{p.SKU, p.Name, p.Brand, p.Price, p.Variants})
		}
		sort.Slice(want, func(i, j int) bool {
			return want[i].Price < want[j].Price || want[i].Price == want[j].Price && want[i].SKU < want[j].SKU
		})
		if len(want) > 100 {
			want = want[:100]
		}
		for _, cs := range cases {
			for rep := 0; rep < runs; rep++ {
				start := time.Now()
				got, e := query(ctx, c, cs.sql, qa...)
				elapsed := time.Since(start).Nanoseconds()
				if e != nil {
					return r, e
				}
				if !reflect.DeepEqual(got, want) {
					return r, fmt.Errorf("%s question %d: %w", cs.mode, qi, errMismatch)
				}
				r.Samples = append(r.Samples, Sample{cs.mode, qi, rep, len(got), digest(got), elapsed})
			}
		}
	}
	if _, _, e = (describe.Handler{Store: s}).Brand(ctx, a, "rename", domain.Brand{ID: "brand-0", Name: "Renamed maker", Version: 1}); e != nil {
		return r, e
	}
	changed, e := query(ctx, c, normalized, questions[0]...)
	if e != nil {
		return r, e
	}
	old, e := query(ctx, c, documentQuery, questions[0]...)
	if e != nil {
		return r, e
	}
	r.Anomaly = !reflect.DeepEqual(changed, old)
	if !r.Anomaly {
		return r, errMismatch
	}
	if e = refresh(ctx, c, base); e != nil {
		return r, e
	}
	r.Repair = true
	for _, cs := range cases {
		got, e := query(ctx, c, cs.sql, questions[0]...)
		if e != nil || !reflect.DeepEqual(got, changed) {
			return r, errMismatch
		}
	}
	if e = nested(ctx, c, a, skus[0], &r); e != nil {
		return r, e
	}
	r.Definitions = []string{"real normalized application rows and isolated temporary flat/JSONB/query-shaped family tables; customer favorites represented by rows and a JSONB document", "all representations match independent Go fixture reductions; partial copied-name rename is deliberately stale until explicit repair", "sqlc v1.30.0 generated query versus actual GORM v1.31.1 lazy associations and eager Preload; statement counts include relationship reads", "all eight plans use ordinary optimizer settings; indexed=false describes comparison-table indexes, not removal of application constraints/indexes", "column-family comparison uses PostgreSQL composite keys and the Go lab uses bbolt ordered keys; neither is a Cassandra/HBase deployment or columnar analytics", "both nested-update targets are temporary and unlogged: WAL is zero by design; local buffer and allocated-relation changes are not device write amplification", "warm database/process/OS state; raw repeated SQL round-trip times include client overhead; no universal model ranking"}
	r.Passed = r.Anomaly && r.Repair && r.Schema && r.Nested.Verified
	return r, nil
}
func nested(ctx context.Context, c *pgx.Conn, tenant, sku string, r *Report) error {
	// Both update targets are temporary and deliberately unlogged. Keep source
	// command/history invariants intact; this is a physical representation drill.
	if _, e := c.Exec(ctx, `CREATE TEMP TABLE model_variant_rows(tenant_id text,sku text,code text,label text,PRIMARY KEY(tenant_id,sku,code))`); e != nil {
		return e
	}
	if _, e := c.Exec(ctx, `INSERT INTO model_variant_rows SELECT tenant_id,sku,code,label FROM catalog.product_variants WHERE tenant_id=$1 AND sku=$2`, tenant, sku); e != nil {
		return e
	}
	// Repeated hash strings are deterministic but deliberately resist TOAST RLE.
	_, e := c.Exec(ctx, `UPDATE model_documents SET doc=jsonb_set(doc,'{padding}',to_jsonb((SELECT string_agg(md5(i::text),'') FROM generate_series(1,2048)i))) WHERE tenant_id=$1 AND sku=$2`, tenant, sku)
	if e != nil {
		return e
	}
	if e = c.QueryRow(ctx, `SELECT pg_total_relation_size('pg_temp.model_documents')`).Scan(&r.Nested.DocumentBefore); e != nil {
		return e
	}
	var p []byte
	if e = c.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) UPDATE model_documents SET doc=jsonb_set(doc,'{variants,0,label}','"Revised option"'::jsonb) WHERE tenant_id=$1 AND sku=$2`, tenant, sku).Scan(&p); e != nil {
		return e
	}
	r.Nested.DocumentWAL = append([]byte(nil), p...)
	if e = c.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) UPDATE model_variant_rows SET label='Revised option' WHERE tenant_id=$1 AND sku=$2 AND code='v-0'`, tenant, sku).Scan(&p); e != nil {
		return e
	}
	r.Nested.RowWAL = append([]byte(nil), p...)
	if e = c.QueryRow(ctx, `SELECT pg_total_relation_size('pg_temp.model_documents')`).Scan(&r.Nested.DocumentAfter); e != nil {
		return e
	}
	// JSONB accepts both field types. The typed contract detects this drift.
	var legacy, current []byte
	if e = c.QueryRow(ctx, `SELECT '{"schema":1,"weight_grams":"120"}'::jsonb,'{"schema":2,"weight_grams":120}'::jsonb`).Scan(&legacy, &current); e != nil {
		return e
	}
	var typed struct {
		Schema int   `json:"schema"`
		Weight int64 `json:"weight_grams"`
	}
	bad := json.Unmarshal(legacy, &typed)
	good := json.Unmarshal(current, &typed)
	r.Schema = bad != nil && good == nil && typed.Weight == 120
	var docLabel, rowLabel, sourceLabel string
	var padding int
	e = c.QueryRow(ctx, `SELECT doc#>>'{variants,0,label}',length(doc->>'padding'),(SELECT label FROM model_variant_rows WHERE tenant_id=$1 AND sku=$2 AND code='v-0'),(SELECT label FROM catalog.product_variants WHERE tenant_id=$1 AND sku=$2 AND code='v-0') FROM model_documents WHERE tenant_id=$1 AND sku=$2`, tenant, sku).Scan(&docLabel, &padding, &rowLabel, &sourceLabel)
	if e != nil {
		return e
	}
	r.Nested.Verified = docLabel == "Revised option" && rowLabel == docLabel && padding == 65536 && sourceLabel == "Option 0"
	if !r.Nested.Verified {
		return errMismatch
	}
	return nil
}
