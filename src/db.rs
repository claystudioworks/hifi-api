use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

pub async fn init_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL DEFAULT '',
            client_id TEXT NOT NULL,
            client_secret TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            user_id TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            notes TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tokens (
            account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            access_token TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            refreshed_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS account_metrics (
            account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            request_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            rate_limit_hits INTEGER NOT NULL DEFAULT 0,
            last_used_at INTEGER,
            last_error_at INTEGER,
            last_error_message TEXT
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cdn_cache (
            track_id TEXT PRIMARY KEY,
            drive_file_id TEXT NOT NULL,
            web_content_link TEXT,
            size_bytes INTEGER,
            created_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_cdn_cache_created ON cdn_cache(created_at)")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sync_jobs (
            id TEXT PRIMARY KEY,
            track_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status)")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_sync_jobs_track ON sync_jobs(track_id)")
        .execute(&pool)
        .await?;

    Ok(pool)
}
