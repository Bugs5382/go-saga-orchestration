DROP TRIGGER IF EXISTS trg_notify_saga_event ON audit.saga_run_events;
DROP FUNCTION IF EXISTS audit.notify_saga_event();
