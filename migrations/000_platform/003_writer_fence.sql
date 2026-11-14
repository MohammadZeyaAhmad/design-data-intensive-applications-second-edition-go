-- The migration owner controls this row. Application roles may execute the
-- trigger but must not own tables, disable triggers, or update this authority.
CREATE TABLE platform.writer_fence (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled boolean NOT NULL DEFAULT false,
    epoch bigint NOT NULL DEFAULT 1 CHECK (epoch > 0)
);
INSERT INTO platform.writer_fence DEFAULT VALUES;
REVOKE ALL ON platform.writer_fence FROM PUBLIC;

CREATE FUNCTION platform.check_writer_epoch() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE authority platform.writer_fence%ROWTYPE;
BEGIN
    -- Hold the shared row lock until commit. Advancing the epoch waits for
    -- already admitted transactions; stale transactions cannot cross it.
    SELECT * INTO STRICT authority FROM platform.writer_fence WHERE singleton FOR SHARE;
    IF authority.enabled AND current_setting('atlas.writer_epoch', true) IS DISTINCT FROM authority.epoch::text THEN
        RAISE EXCEPTION 'inventory writer epoch rejected' USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END $$;
