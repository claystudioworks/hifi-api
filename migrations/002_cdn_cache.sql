-- CDN cache: maps Tidal trackId -> Drive Worker file
CREATE TABLE IF NOT EXISTS cdn_cache (
    track_id TEXT PRIMARY KEY,
    drive_file_id TEXT NOT NULL,
    web_content_link TEXT,
    size_bytes INTEGER,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cdn_cache_created ON cdn_cache(created_at);
