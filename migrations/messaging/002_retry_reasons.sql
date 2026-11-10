ALTER TABLE messaging.retry_audit DROP CONSTRAINT retry_audit_reason_check;
ALTER TABLE messaging.retry_audit ADD CHECK(reason IN('consumer_upgraded','source_verified'));
