-- Stable across physical replication; a separately initialized database gets
-- another identity. History changes use the existing writer-fence epoch.
CREATE TABLE platform.read_source (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    source_id uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO platform.read_source DEFAULT VALUES;
REVOKE ALL ON platform.read_source FROM PUBLIC;

CREATE FUNCTION platform.read_identity() RETURNS TABLE(source_id uuid, epoch bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT r.source_id, f.epoch FROM platform.read_source r
    CROSS JOIN platform.writer_fence f WHERE r.singleton AND f.singleton
$$;
