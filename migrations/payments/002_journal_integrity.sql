ALTER TABLE payments.intents ADD CONSTRAINT payment_money_identity UNIQUE(tenant_id,id,amount_minor,currency);
ALTER TABLE payments.journal ADD CONSTRAINT journal_money_matches_intent FOREIGN KEY(tenant_id,payment_id,amount_minor,currency) REFERENCES payments.intents(tenant_id,id,amount_minor,currency);
CREATE FUNCTION payments.fixed_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.tenant_id,NEW.id,NEW.amount_minor,NEW.currency,NEW.method) IS DISTINCT FROM (OLD.tenant_id,OLD.id,OLD.amount_minor,OLD.currency,OLD.method) THEN
        RAISE EXCEPTION 'payment identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER fixed_payment_identity BEFORE UPDATE ON payments.intents FOR EACH ROW EXECUTE FUNCTION payments.fixed_identity();
CREATE TRIGGER fixed_provider_identity BEFORE UPDATE ON provider.operations FOR EACH ROW EXECUTE FUNCTION payments.fixed_identity();
CREATE FUNCTION payments.refund_requires_capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind='refund' AND NOT EXISTS(SELECT 1 FROM payments.journal WHERE tenant_id=NEW.tenant_id AND payment_id=NEW.payment_id AND kind='capture') THEN
        RAISE EXCEPTION 'refund requires a recorded capture' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER valid_refund AFTER INSERT ON payments.journal DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION payments.refund_requires_capture();
