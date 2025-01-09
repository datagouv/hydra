-- Add indexes to improve filtering and batch selection queries
CREATE INDEX IF NOT EXISTS status_deleted_idx ON catalog(status, deleted);
CREATE INDEX IF NOT EXISTS last_check_priority_idx ON catalog(last_check, priority);
