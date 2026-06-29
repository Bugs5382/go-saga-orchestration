-- Records why a run reached a terminal failed/cancelled state (the failing
-- step's error message, or the cancel reason) so a run is self-describing
-- without diffing its event log. See issue #80.
ALTER TABLE runtime.saga_runs
  ADD COLUMN last_error TEXT NULL;
