-- Existing holds receive a fifteen-minute grace period from this migration.
ALTER TABLE inventory.reservations ADD COLUMN expires_at timestamptz NOT NULL
    DEFAULT (now() + interval '15 minutes');
ALTER TABLE inventory.reservations ADD CONSTRAINT reservations_finite_expiry CHECK (isfinite(expires_at));
ALTER TABLE inventory.reservations DROP CONSTRAINT reservations_status_check;
ALTER TABLE inventory.reservations ADD CONSTRAINT reservations_status_check
    CHECK (status IN ('active', 'committed', 'released', 'expired', 'reversed'));
CREATE INDEX reservations_due ON inventory.reservations (expires_at, tenant_id, id)
    WHERE status = 'active';
