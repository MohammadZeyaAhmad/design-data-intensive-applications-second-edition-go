-- atlas:bounded-locks
-- Cache invalidation is advisory and delivered only after the price transaction
-- commits. Missed notifications are covered by bounded display-cache TTLs.
CREATE FUNCTION catalog.notify_product_cache() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version > OLD.version THEN
        PERFORM pg_notify('atlas_catalog_cache_v1', json_build_object(
            'tenant', NEW.tenant_id, 'sku', NEW.sku, 'version', NEW.version
        )::text);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER catalog_product_cache_changed
AFTER UPDATE OF version ON catalog.products
FOR EACH ROW EXECUTE FUNCTION catalog.notify_product_cache();
