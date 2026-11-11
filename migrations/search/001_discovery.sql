CREATE SCHEMA search;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE search.documents (
  tenant_id text NOT NULL, id text NOT NULL, version bigint NOT NULL CHECK(version>=0 AND version<=1000000000000),
  sku text NOT NULL DEFAULT '', warehouse text NOT NULL DEFAULT '', title text NOT NULL DEFAULT '', body text NOT NULL DEFAULT '',
  longitude double precision, latitude double precision,
  location point GENERATED ALWAYS AS (CASE WHEN longitude IS NULL THEN NULL ELSE point(longitude,latitude) END) STORED,
  terms tsvector GENERATED ALWAYS AS (setweight(to_tsvector('english'::regconfig,title),'A') || setweight(to_tsvector('english'::regconfig,body),'B')) STORED,
  deleted boolean NOT NULL DEFAULT true, data jsonb NOT NULL DEFAULT '{}', metadata jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY(tenant_id,id),
  CHECK ((longitude IS NULL AND latitude IS NULL) OR (longitude IS NOT NULL AND latitude IS NOT NULL AND longitude>=-180 AND longitude<=180 AND latitude>=-90 AND latitude<=90)),
  CHECK (octet_length(title)<=256 AND octet_length(body)<=4096),
  CHECK (deleted OR length(title)>0)
);
CREATE INDEX search_terms_gin ON search.documents USING gin(terms) WHERE NOT deleted;
CREATE INDEX search_title_trgm ON search.documents USING gin(title gin_trgm_ops) WHERE NOT deleted;
CREATE INDEX search_location_gist ON search.documents USING gist(location) WHERE NOT deleted AND location IS NOT NULL;
CREATE TABLE search.document_history(tenant_id text NOT NULL,id text NOT NULL,version bigint NOT NULL,body jsonb NOT NULL,metadata jsonb NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,id,version));
CREATE FUNCTION search.immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'search history is immutable' USING ERRCODE='55000'; END; $$;
CREATE TRIGGER immutable_history BEFORE UPDATE OR DELETE ON search.document_history FOR EACH ROW EXECUTE FUNCTION search.immutable();
CREATE TABLE search.command_results(tenant_id text NOT NULL,operation text NOT NULL,idempotency_key text NOT NULL,request_hash bytea NOT NULL,response jsonb,PRIMARY KEY(tenant_id,operation,idempotency_key));
