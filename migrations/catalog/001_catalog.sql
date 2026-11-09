CREATE SCHEMA catalog;
CREATE TABLE catalog.products (tenant_id text NOT NULL,sku text NOT NULL,version bigint NOT NULL DEFAULT 0 CHECK(version>=0),PRIMARY KEY(tenant_id,sku));
CREATE TABLE catalog.prices (tenant_id text NOT NULL,sku text NOT NULL,version bigint NOT NULL CHECK(version>0),name text NOT NULL,unit_price_minor bigint NOT NULL CHECK(unit_price_minor>0 AND unit_price_minor<=1000000000000),currency text NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),PRIMARY KEY(tenant_id,sku,version),FOREIGN KEY(tenant_id,sku) REFERENCES catalog.products);
CREATE FUNCTION catalog.immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'catalog price/quote history is immutable' USING ERRCODE='55000'; END; $$;
CREATE TRIGGER immutable_prices BEFORE UPDATE OR DELETE ON catalog.prices FOR EACH ROW EXECUTE FUNCTION catalog.immutable();
CREATE TABLE catalog.quotes (tenant_id text NOT NULL,id uuid NOT NULL,body jsonb NOT NULL,expires_at timestamptz NOT NULL,PRIMARY KEY(tenant_id,id));
CREATE TRIGGER immutable_quotes BEFORE UPDATE OR DELETE ON catalog.quotes FOR EACH ROW EXECUTE FUNCTION catalog.immutable();
CREATE TABLE catalog.command_results (tenant_id text NOT NULL,operation text NOT NULL,idempotency_key text NOT NULL,request_hash bytea NOT NULL,response jsonb,PRIMARY KEY(tenant_id,operation,idempotency_key));
