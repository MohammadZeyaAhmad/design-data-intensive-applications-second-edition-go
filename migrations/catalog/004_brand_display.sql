-- atlas:bounded-locks
-- Expand only. Backfill, validation and destructive contraction are separate.
ALTER TABLE catalog.brands ADD COLUMN display_name text
 CHECK (display_name IS NULL OR octet_length(display_name) BETWEEN 1 AND 200);
ALTER TABLE catalog.content_history ADD COLUMN schema_version integer NOT NULL DEFAULT 1 CHECK(schema_version>0);

CREATE FUNCTION catalog.bridge_brand_names() RETURNS trigger LANGUAGE plpgsql AS $bridge$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.display_name IS NULL THEN NEW.display_name:=NEW.name;
  ELSIF NEW.name IS NULL THEN NEW.name:=NEW.display_name;
  ELSIF NEW.name IS DISTINCT FROM NEW.display_name THEN RAISE EXCEPTION 'inconsistent brand names' USING ERRCODE='23514';
  END IF;
 ELSE
  IF NEW.name IS DISTINCT FROM OLD.name AND NEW.display_name IS NOT DISTINCT FROM OLD.display_name THEN NEW.display_name:=NEW.name;
  ELSIF NEW.display_name IS DISTINCT FROM OLD.display_name AND NEW.name IS NOT DISTINCT FROM OLD.name THEN NEW.name:=NEW.display_name;
  ELSIF NEW.display_name IS NULL THEN NEW.display_name:=NEW.name;
  ELSIF NEW.name IS DISTINCT FROM NEW.display_name THEN RAISE EXCEPTION 'inconsistent brand names' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $bridge$;
CREATE TRIGGER bridge_brand_names BEFORE INSERT OR UPDATE ON catalog.brands FOR EACH ROW EXECUTE FUNCTION catalog.bridge_brand_names();
CREATE VIEW catalog.brand_names AS
 SELECT tenant_id,id,COALESCE(display_name,name,'') AS display_name,version FROM catalog.brands;

CREATE TABLE catalog.brand_rollout (
 id integer PRIMARY KEY CHECK(id=1), phase text NOT NULL DEFAULT 'expanded' CHECK(phase IN ('expanded','backfilled','validated','contracted')),
 batches bigint NOT NULL DEFAULT 0 CHECK(batches>=0), rows_backfilled bigint NOT NULL DEFAULT 0 CHECK(rows_backfilled>=0),
 next_batch_at timestamptz NOT NULL DEFAULT 'epoch', rollback_until timestamptz, rollback_retired_at timestamptz
);
INSERT INTO catalog.brand_rollout(id) VALUES(1);
CREATE TABLE catalog.brand_backfill_receipts (
 batch_id text PRIMARY KEY, rows_backfilled integer NOT NULL CHECK(rows_backfilled BETWEEN 0 AND 128), pending boolean NOT NULL, batch_limit integer NOT NULL CHECK(batch_limit BETWEEN 1 AND 128), min_interval_ms bigint NOT NULL CHECK(min_interval_ms BETWEEN 10 AND 60000),
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE catalog.brand_schema_readers (
 id text PRIMARY KEY, capability integer NOT NULL CHECK(capability IN (1,2)), lease_until timestamptz NOT NULL, retired_at timestamptz
);
CREATE TABLE catalog.brand_rollout_audit (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, action text NOT NULL, operator text NOT NULL, reason text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER immutable_brand_rollout_audit BEFORE UPDATE OR DELETE ON catalog.brand_rollout_audit FOR EACH ROW EXECUTE FUNCTION catalog.immutable();

CREATE TRIGGER immutable_brand_backfill_receipts BEFORE UPDATE OR DELETE ON catalog.brand_backfill_receipts FOR EACH ROW EXECUTE FUNCTION catalog.immutable();
