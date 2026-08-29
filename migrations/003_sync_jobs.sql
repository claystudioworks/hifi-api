-- Bulk sync job queue: one row per track to download + upload to Drive Worker
CREATE TABLE IF NOT EXISTS sync_jobs (
    id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending, downloading, uploading, done, failed
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_track ON sync_jobs(track_id);
