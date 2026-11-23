ALTER TABLE evaluation_snapshots DROP CONSTRAINT evaluation_snapshots_hosts_check;
ALTER TABLE evaluation_snapshots ADD CONSTRAINT evaluation_snapshots_hosts_check
  CHECK (cardinality(hosts) BETWEEN 1 AND 16 OR (cardinality(hosts) = 0 AND NOT termuxpp));
