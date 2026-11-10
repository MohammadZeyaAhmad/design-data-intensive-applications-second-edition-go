CREATE SCHEMA messaging;
CREATE TABLE messaging.inbox (
    topic text NOT NULL, message_id uuid NOT NULL, tenant_id text NOT NULL,
    stream_id text NOT NULL, sequence bigint NOT NULL CHECK(sequence>0),
    envelope jsonb NOT NULL, body_hash bytea NOT NULL CHECK(octet_length(body_hash)=32),
    status text NOT NULL DEFAULT 'pending' CHECK(status IN('pending','done','quarantined')),
    attempts bigint NOT NULL DEFAULT 0, cycle_attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(), applied_at timestamptz,
    last_error text, control_version bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(topic,message_id), UNIQUE(topic,tenant_id,stream_id,sequence)
);
CREATE INDEX inbox_ready ON messaging.inbox(available_at,topic,tenant_id,stream_id,sequence) WHERE status='pending';
CREATE TABLE messaging.deliveries (
    group_id text NOT NULL, topic text NOT NULL, partition integer NOT NULL, offset_id bigint NOT NULL,
    body_hash bytea NOT NULL CHECK(octet_length(body_hash)=32), received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(group_id,topic,partition,offset_id)
);
CREATE TABLE messaging.rejected (
    group_id text NOT NULL, topic text NOT NULL, partition integer NOT NULL, offset_id bigint NOT NULL,
    record_key bytea NOT NULL, record_value bytea NOT NULL, reason text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(group_id,topic,partition,offset_id)
);
CREATE TABLE messaging.command_results (
    tenant_id text NOT NULL,operation text NOT NULL,idempotency_key text NOT NULL,
    request_hash bytea NOT NULL CHECK(octet_length(request_hash)=32),response jsonb,
    PRIMARY KEY(tenant_id,operation,idempotency_key)
);
CREATE TABLE messaging.retry_audit (
    tenant_id text NOT NULL,topic text NOT NULL,message_id uuid NOT NULL,version bigint NOT NULL,
    reason text NOT NULL CHECK(reason='consumer_upgraded'),
    metadata jsonb NOT NULL CHECK(platform.valid_metadata(metadata)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,topic,message_id,version)
);
CREATE FUNCTION messaging.immutable_fact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'immutable broker receipt or audit'; END $$;
CREATE TRIGGER deliveries_immutable BEFORE UPDATE OR DELETE ON messaging.deliveries FOR EACH ROW EXECUTE FUNCTION messaging.immutable_fact();
CREATE TRIGGER rejects_immutable BEFORE UPDATE OR DELETE ON messaging.rejected FOR EACH ROW EXECUTE FUNCTION messaging.immutable_fact();
CREATE TRIGGER retry_immutable BEFORE UPDATE OR DELETE ON messaging.retry_audit FOR EACH ROW EXECUTE FUNCTION messaging.immutable_fact();
CREATE FUNCTION messaging.protect_envelope() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.topic,NEW.message_id,NEW.tenant_id,NEW.stream_id,NEW.sequence,NEW.envelope,NEW.body_hash)
      IS DISTINCT FROM ROW(OLD.topic,OLD.message_id,OLD.tenant_id,OLD.stream_id,OLD.sequence,OLD.envelope,OLD.body_hash)
    THEN RAISE EXCEPTION 'immutable transport identity and payload'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER envelope_immutable BEFORE UPDATE ON messaging.inbox FOR EACH ROW EXECUTE FUNCTION messaging.protect_envelope();
