ALTER TABLE evaluation_snapshots
  ADD COLUMN preferred_pool TEXT,
  ADD COLUMN preferred_agent UUID,
  ADD COLUMN front BOOLEAN NOT NULL DEFAULT false,
  ADD CONSTRAINT snapshot_single_worker_preference
    CHECK (preferred_pool IS NULL OR preferred_agent IS NULL);
