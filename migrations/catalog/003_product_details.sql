CREATE TABLE catalog.brands (
 tenant_id text NOT NULL,id text NOT NULL,name text NOT NULL CHECK(octet_length(name) BETWEEN 1 AND 200),version bigint NOT NULL CHECK(version>0),PRIMARY KEY(tenant_id,id)
);
CREATE TABLE catalog.product_details (
 tenant_id text NOT NULL,sku text NOT NULL,brand_id text NOT NULL,description text NOT NULL CHECK(octet_length(description)<=4096),
 attributes jsonb NOT NULL CHECK(jsonb_typeof(attributes)='object' AND octet_length(attributes::text)<=8192),version bigint NOT NULL CHECK(version>0),
 PRIMARY KEY(tenant_id,sku),FOREIGN KEY(tenant_id,sku) REFERENCES catalog.products,FOREIGN KEY(tenant_id,brand_id) REFERENCES catalog.brands
);
CREATE INDEX catalog_details_brand ON catalog.product_details(tenant_id,brand_id,sku);
CREATE INDEX catalog_details_attributes ON catalog.product_details USING gin(attributes jsonb_path_ops);
CREATE TABLE catalog.product_variants (
 tenant_id text NOT NULL,sku text NOT NULL,code text NOT NULL,label text NOT NULL,position integer NOT NULL CHECK(position BETWEEN 0 AND 31),
 PRIMARY KEY(tenant_id,sku,code),UNIQUE(tenant_id,sku,position),FOREIGN KEY(tenant_id,sku) REFERENCES catalog.product_details
);
CREATE TABLE catalog.product_categories (
 tenant_id text NOT NULL,sku text NOT NULL,category text NOT NULL,PRIMARY KEY(tenant_id,sku,category),FOREIGN KEY(tenant_id,sku) REFERENCES catalog.product_details
);
CREATE INDEX catalog_products_by_category ON catalog.product_categories(tenant_id,category,sku);
CREATE TABLE catalog.content_history (
 tenant_id text NOT NULL,kind text NOT NULL CHECK(kind IN ('brand','details')),id text NOT NULL,version bigint NOT NULL,body jsonb NOT NULL,
 metadata jsonb NOT NULL CHECK(platform.valid_metadata(metadata)),recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,kind,id,version)
);
CREATE TRIGGER immutable_content_history BEFORE UPDATE OR DELETE ON catalog.content_history FOR EACH ROW EXECUTE FUNCTION catalog.immutable();
