CREATE OR REPLACE FUNCTION audit.notify_saga_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Channel names must be a valid identifier; use the run_id stripped of dashes
    -- to stay well under Postgres' 63-byte channel-name limit.
    PERFORM pg_notify('saga_event_' || replace(NEW.run_id::text, '-', ''), NEW.id::text);
    RETURN NEW;
END $$;

CREATE TRIGGER trg_notify_saga_event
AFTER INSERT ON audit.saga_run_events
FOR EACH ROW
EXECUTE FUNCTION audit.notify_saga_event();
