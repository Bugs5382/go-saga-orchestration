ALTER TABLE runtime.saga_runs
  ADD COLUMN wakeup_at              TIMESTAMPTZ NULL,
  ADD COLUMN awaited_signal         TEXT NULL,
  ADD COLUMN awaited_event_topic    TEXT NULL,
  ADD COLUMN awaited_event_headers  JSONB NULL;

CREATE INDEX idx_saga_runs_wakeup_due
  ON runtime.saga_runs (wakeup_at)
  WHERE state = 'paused' AND wakeup_at IS NOT NULL;

CREATE INDEX idx_saga_runs_awaited_topic
  ON runtime.saga_runs (awaited_event_topic)
  WHERE state = 'paused' AND awaited_event_topic IS NOT NULL;
