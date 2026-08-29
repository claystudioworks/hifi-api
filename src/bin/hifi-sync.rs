//! hifi-sync — bulk downloader daemon for Drive CDN Worker
//!
//! Polls `sync_jobs` (pending → downloading → uploading → done) with
//! **1 concurrent download + 1 concurrent upload**, **3-8s jitter** between
//! tracks, and daily 750GB guard. All Tidal hits go through the local
//! `hifi-api` (so per-account 1 rps burst 3 is enforced) — no direct Tidal
//! calls, no ban risk.

use anyhow::Result;
use base64::Engine;
use rand::Rng;
use serde_json::Value;
use sqlx::SqlitePool;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "hifi.db".to_string());
    let hifi_api_url = std::env::var("HIFI_API_URL")
        .or_else(|_| std::env::var("HIFI_API"))
        .unwrap_or_else(|_| "http://localhost:8000".to_string());
    let hifi_api_url = hifi_api_url.trim_end_matches('/').to_string();

    // Drive Worker config — optional; if not set, we just cache the Tidal URL
    let worker_url = std::env::var("CDN_WORKER_URL")
        .or_else(|_| std::env::var("DRIVE_WORKER_URL"))
        .ok();
    let worker_token = std::env::var("CDN_WORKER_TOKEN")
        .or_else(|_| std::env::var("DRIVE_WORKER_TOKEN"))
        .unwrap_or_default();

    tracing::info!("hifi-sync starting — hifi-api={} worker={:?}", hifi_api_url, worker_url);

    let pool = SqlitePool::connect(&database_url).await?;
    sqlx::migrate!().run(&pool).await?;
    tracing::info!("DB migrated at {}", database_url);

    let http = reqwest::Client::builder()
        .user_agent("hifi-sync/2.10")
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    // Optional DriveWorker client
    let drive_client = worker_url.as_ref().map(|url| {
        let url = url.trim_end_matches('/').to_string();
        (url, worker_token.clone())
    });

    loop {
        // Check daily guards
        let bytes_today = hifi_api::cdn::bytes_uploaded_today(&pool).await;
        if bytes_today > 750 * 1024 * 1024 * 1024 {
            tracing::warn!("daily 750GB limit reached ({} bytes), sleeping 1h", bytes_today);
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            continue;
        }
        let pending: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='pending'")
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
        if pending == 0 {
            tracing::debug!("no pending jobs — sleep 5s");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        tracing::info!("{} pending jobs, bytes_today={}", pending, bytes_today);

        // Fetch one pending job (oldest first)
        let job: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT id, track_id, attempts FROM sync_jobs WHERE status='pending' ORDER BY created_at ASC LIMIT 1",
        )
        .fetch_optional(&pool)
        .await?;

        let Some((job_id, track_id, attempts)) = job else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };

        if attempts > 3 {
            tracing::warn!("job {} track {} exceeded 3 attempts — marking failed", job_id, track_id);
            sqlx::query("UPDATE sync_jobs SET status='failed', last_error='too many attempts', updated_at=? WHERE id=?")
                .bind(chrono::Utc::now().timestamp())
                .bind(&job_id)
                .execute(&pool)
                .await?;
            continue;
        }

        // Mark downloading
        sqlx::query("UPDATE sync_jobs SET status='downloading', attempts=attempts+1, updated_at=? WHERE id=?")
            .bind(chrono::Utc::now().timestamp())
            .bind(&job_id)
            .execute(&pool)
            .await?;

        // Anti-ban jitter 3-8s before hit hifi-api
        let jitter = rand::thread_rng().gen_range(3000..8000);
        tracing::info!("job {} track {} — jitter {}ms before Tidal", job_id, track_id, jitter);
        tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;

        match process_track(&pool, &http, &hifi_api_url, &drive_client, &track_id).await {
            Ok((drive_id, _link, size)) => {
                tracing::info!("job {} track {} done — drive_id={} size={:?}", job_id, track_id, drive_id, size);
                // Cache already inserted in process_track, now mark job done
                sqlx::query("UPDATE sync_jobs SET status='done', updated_at=? WHERE id=?")
                    .bind(chrono::Utc::now().timestamp())
                    .bind(&job_id)
                    .execute(&pool)
                    .await?;
                // Metrics
                hifi_api::metrics::requests()
                    .with_label_values(&["hifi-sync", "done"])
                    .inc();
            }
            Err(e) => {
                tracing::warn!("job {} track {} failed: {}", job_id, track_id, e);
                let msg = format!("{}", e);
                sqlx::query("UPDATE sync_jobs SET status='pending', last_error=?, updated_at=? WHERE id=?")
                    .bind(&msg)
                    .bind(chrono::Utc::now().timestamp())
                    .bind(&job_id)
                    .execute(&pool)
                    .await?;
                // Backoff 30s + jitter on failure
                let backoff = rand::thread_rng().gen_range(30000..60000);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                hifi_api::metrics::requests()
                    .with_label_values(&["hifi-sync", "failed"])
                    .inc();
            }
        }

        // Small gap before next job (ensures 1 concurrent)
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn process_track(
    pool: &SqlitePool,
    http: &reqwest::Client,
    hifi_api_url: &str,
    drive_client: &Option<(String, String)>,
    track_id: &str,
) -> Result<(String, String, Option<i64>)> {
    // 1. Fetch via hifi-api (goes through per-account limiter + cache)
    let track_url = format!("{}/track/?id={}&quality=LOSSLESS", hifi_api_url, track_id);
    tracing::debug!("fetching {}", track_url);
    let resp = http.get(&track_url).send().await?;
    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("hifi-api track fetch failed: {}", body);
    }
    let v: Value = resp.json().await?;
    let data = v.get("data").ok_or_else(|| anyhow::anyhow!("missing data in track response"))?;
    let manifest_b64 = data.get("manifest").and_then(|m| m.as_str()).unwrap_or("");
    let _manifest_type = data.get("manifestMimeType").and_then(|m| m.as_str()).unwrap_or("");

    // Decode manifest to get actual FLAC URL
    let flac_url = if !manifest_b64.is_empty() {
        // Try base64 JSON (LOSSLESS)
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(manifest_b64) {
            if let Ok(j) = serde_json::from_slice::<Value>(&decoded) {
                if let Some(urls) = j.get("urls").and_then(|u| u.as_array()) {
                    if let Some(u) = urls.first().and_then(|x| x.as_str()) {
                        u.to_string()
                    } else {
                        anyhow::bail!("no urls in manifest");
                    }
                } else if let Some(url) = j.get("url").and_then(|u| u.as_str()) {
                    url.to_string()
                } else {
                    // DASH manifest — not single file, use fallback: store manifest URL itself as CDN (worker will proxy)
                    // For bulk we want single file, so we bail for DASH and let caller retry with fallback
                    anyhow::bail!("DASH manifest not supported for bulk — use LOSSLESS");
                }
            } else {
                // Maybe manifest is already URL (not base64)
                manifest_b64.to_string()
            }
        } else {
            manifest_b64.to_string()
        }
    } else {
        anyhow::bail!("no manifest in track response");
    };

    // 2. Download FLAC bytes (stream)
    tracing::info!("downloading FLAC for track {} from {}", track_id, &flac_url[..80.min(flac_url.len())]);
    let flac_resp = http.get(&flac_url).send().await?;
    if !flac_resp.status().is_success() {
        anyhow::bail!("FLAC download failed {}", flac_resp.status());
    }
    let bytes = flac_resp.bytes().await?.to_vec();
    let size = bytes.len() as i64;
    if bytes.is_empty() {
        anyhow::bail!("empty FLAC download");
    }
    tracing::info!("downloaded {} bytes for track {}", size, track_id);

    // 3. Upload to Drive Worker if configured, else just cache the Tidal URL
    if let Some((worker_url, token)) = drive_client {
        let client = hifi_api::cdn::DriveWorkerClient {
            base_url: worker_url.clone(),
            api_token: token.clone(),
            http: http.clone(),
        };
        let file_name = format!("{}.flac", track_id);
        let (drive_id, link) = client
            .upload_file(&file_name, "audio/flac", bytes)
            .await
            .map_err(|e| anyhow::anyhow!("drive upload failed: {}", e))?;
        // Cache
        hifi_api::cdn::put_cache(pool, track_id, &drive_id, &link, Some(size)).await?;
        Ok((drive_id, link, Some(size)))
    } else {
        // No worker — cache the Tidal URL directly (still benefits from CDN redirect)
        let fake_id = format!("tidal_{}", track_id);
        let link = flac_url.clone();
        hifi_api::cdn::put_cache(pool, track_id, &fake_id, &link, Some(size)).await?;
        Ok((fake_id, link, Some(size)))
    }
}
