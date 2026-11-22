CREATE TABLE evaluation_snapshots (
  evaluation_id UUID PRIMARY KEY REFERENCES evaluations(id) ON DELETE CASCADE,
  archive BYTEA NOT NULL CHECK (octet_length(archive) BETWEEN 1 AND 16777216),
  hosts TEXT[] NOT NULL CHECK (cardinality(hosts) BETWEEN 1 AND 16),
  termuxpp BOOLEAN NOT NULL,
  priority INTEGER NOT NULL CHECK (priority IN (0, 10))
);

ALTER TABLE evaluations DROP CONSTRAINT evaluations_trigger_kind_check;
ALTER TABLE evaluations ADD CONSTRAINT evaluations_trigger_kind_check
  CHECK (trigger_kind IN ('source_change', 'manual', 'interval', 'snapshot'));
