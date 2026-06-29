ALTER TABLE definitions.action_registry
  DROP COLUMN IF EXISTS address,
  DROP COLUMN IF EXISTS transport;
