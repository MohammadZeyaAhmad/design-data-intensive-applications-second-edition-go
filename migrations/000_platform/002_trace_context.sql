-- atlas:bounded-locks
-- Reader expansion only. Producers keep durable trace context disabled until
-- every consumer understands the optional field; existing rows are unchanged.
CREATE OR REPLACE FUNCTION platform.valid_metadata(m jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT coalesce(m='{"version":0}'::jsonb OR (
    jsonb_typeof(m)='object' AND m->'version'='1'::jsonb
    AND m ?& ARRAY['version','actor','correlation_id','causation_id']
    AND m-ARRAY['version','actor','correlation_id','causation_id','origin_request_id','traceparent']='{}'::jsonb
    AND m->>'actor' ~ '^(tenant-key:[a-z][a-z0-9-]{0,63}|worker:[a-z][a-z0-9-]{0,63}|operator:local|system:internal)$'
    AND m->>'correlation_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND m->>'causation_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND (NOT m ? 'origin_request_id' OR m->>'origin_request_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    AND (NOT m ? 'traceparent' OR (
        m->>'traceparent' ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$'
        AND substring(m->>'traceparent',4,32)<>repeat('0',32)
        AND substring(m->>'traceparent',37,16)<>repeat('0',16)
    ))
),false);
$$;
