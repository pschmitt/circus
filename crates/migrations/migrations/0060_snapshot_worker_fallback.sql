ALTER TABLE evaluation_snapshots
  ADD COLUMN worker_fallback BOOLEAN NOT NULL DEFAULT false,
  ADD CONSTRAINT snapshot_fallback_requires_agent
    CHECK (NOT worker_fallback OR preferred_agent IS NOT NULL);
