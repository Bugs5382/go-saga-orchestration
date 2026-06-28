-- issue #59: optional dispatch descriptor on action registrations.
-- transport: 'grpc' (default) | 'http' | 'rmq'; address: callback URL (http)
-- or queue name (rmq), required only for the non-gRPC transports.
ALTER TABLE definitions.action_registry
  ADD COLUMN transport TEXT,
  ADD COLUMN address   TEXT;
