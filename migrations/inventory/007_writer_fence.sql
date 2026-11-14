-- Statement triggers guard effects and receipts together, including workers.
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.stock
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.reservations
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.command_results
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.outbox
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.stock_views
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.projection_receipts
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
CREATE TRIGGER writer_fence BEFORE INSERT OR UPDATE OR DELETE ON inventory.workflow_steps
FOR EACH STATEMENT EXECUTE FUNCTION platform.check_writer_epoch();
