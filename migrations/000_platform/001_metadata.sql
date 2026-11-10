CREATE SCHEMA platform;
CREATE FUNCTION platform.valid_metadata(m jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT coalesce(m='{"version":0}'::jsonb OR (
    jsonb_typeof(m)='object' AND m->'version'='1'::jsonb
    AND m ?& ARRAY['version','actor','correlation_id','causation_id']
    AND m-ARRAY['version','actor','correlation_id','causation_id','origin_request_id']='{}'::jsonb
    AND m->>'actor' ~ '^(tenant-key:[a-z][a-z0-9-]{0,63}|worker:[a-z][a-z0-9-]{0,63}|operator:local|system:internal)$'
    AND m->>'correlation_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND m->>'causation_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND (NOT m ? 'origin_request_id' OR m->>'origin_request_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
),false);
$$;
CREATE FUNCTION platform.protect_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.metadata IS DISTINCT FROM OLD.metadata THEN RAISE EXCEPTION 'immutable origin metadata'; END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION platform.protect_once_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.cancel_metadata->>'version'='1' AND NEW.cancel_metadata IS DISTINCT FROM OLD.cancel_metadata
    THEN RAISE EXCEPTION 'immutable cancellation provenance'; END IF;
    RETURN NEW;
END $$;
