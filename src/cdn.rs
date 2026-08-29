//! CDN layer — Google Drive Worker (tas33n) + fallback to Tidal.
//!
//! `hifi-sync` daemon uses `DriveWorkerClient::upload_file` to POST FLAC
//! to `https://cdn.yourdomain.com/api/files`. `hifi-api` then serves
//! `GET /cdn/{trackId}` by 302 redirecting to the Worker's
//! `/files/{driveFileId}` (Cloudflare edge cached, Range support).
//! If not cached, falls back to Tidal manifest.

use sqlx::SqlitePool;

/// Get cached Drive Worker link for a track, if any.
pub async fn get_cached_link(pool: &SqlitePool, track_id: &str) -> Option<String> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT web_content_link FROM cdn_cache WHERE track_id = ?")
            .bind(track_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.map(|r| r.0)
}

/// Get cached drive file id.
pub async fn get_cached_file_id(pool: &SqlitePool, track_id: &str) -> Option<String> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT drive_file_id FROM cdn_cache WHERE track_id = ?")
            .bind(track_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.map(|r| r.0)
}

pub async fn put_cache(
    pool: &SqlitePool,
    track_id: &str,
    drive_file_id: &str,
    link: &str,
    size_bytes: Option<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT OR REPLACE INTO cdn_cache(track_id, drive_file_id, web_content_link, size_bytes, created_at) VALUES(?,?,?,?,?)",
    )
    .bind(track_id)
    .bind(drive_file_id)
    .bind(link)
    .bind(size_bytes)
    .bind(chrono::Utc::now().timestamp())
    .execute(pool)
    .await
    .map(|_| ())
}

/// Insert a sync job (idempotent per track_id pending/done).
pub async fn enqueue_track(pool: &SqlitePool, track_id: &str) -> Result<bool, sqlx::Error> {
    // Avoid duplicate pending/done for same track
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT id FROM sync_jobs WHERE track_id = ? AND status IN ('pending','downloading','uploading','done')")
            .bind(track_id)
            .fetch_optional(pool)
            .await?;
    if existing.is_some() {
        return Ok(false);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO sync_jobs(id, track_id, status, attempts, created_at, updated_at) VALUES(?,?,?,?,?,?)",
    )
    .bind(&id)
    .bind(track_id)
    .bind("pending")
    .bind(0)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(true)
}

/// Count total bytes uploaded today (for 750GB guard).
pub async fn bytes_uploaded_today(pool: &SqlitePool) -> i64 {
    let today_start = chrono::Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();
    let row: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT SUM(size_bytes) FROM cdn_cache WHERE created_at >= ?")
            .bind(today_start)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.and_then(|r| r.0).unwrap_or(0)
}

/// Drive Worker client — talks to tas33n/Google-drive-cdn-worker
pub struct DriveWorkerClient {
    pub base_url: String,
    pub api_token: String,
    pub http: reqwest::Client,
}

impl DriveWorkerClient {
    pub fn from_env(http: reqwest::Client) -> Option<Self> {
        let base_url = std::env::var("CDN_WORKER_URL")
            .or_else(|_| std::env::var("DRIVE_WORKER_URL"))
            .ok()?;
        let api_token = std::env::var("CDN_WORKER_TOKEN")
            .or_else(|_| std::env::var("DRIVE_WORKER_TOKEN"))
            .or_else(|_| std::env::var("API_TOKENS"))
            .unwrap_or_default();
        if base_url.is_empty() {
            return None;
        }
        Some(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_token,
            http,
        })
    }

    /// Upload a FLAC file bytes to Worker via multipart POST /api/files
    pub async fn upload_file(
        &self,
        file_name: &str,
        mime_type: &str,
        bytes: Vec<u8>,
    ) -> Result<(String, String), String> {
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(file_name.to_string())
            .mime_str(mime_type)
            .map_err(|e| e.to_string())?;
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("metadata", format!("{{\"name\":\"{}\"}}", file_name));

        let mut req = self
            .http
            .post(format!("{}/api/files", self.base_url))
            .multipart(form);
        if !self.api_token.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", self.api_token));
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("worker upload failed {}: {}", status, body));
        }
        let id = body["data"]["id"]
            .as_str()
            .or_else(|| body["id"].as_str())
            .ok_or("missing id in worker response")?
            .to_string();
        let raw_url = body["data"]["rawUrl"]
            .as_str()
            .or_else(|| body["rawUrl"].as_str())
            .unwrap_or("");
        let link = if raw_url.is_empty() {
            format!("{}/files/{}", self.base_url, id)
        } else {
            raw_url.to_string()
        };
        Ok((id, link))
    }

    /// Build public CDN URL for a drive file id
    pub fn public_url(&self, drive_file_id: &str) -> String {
        format!("{}/files/{}", self.base_url, drive_file_id)
    }
}
