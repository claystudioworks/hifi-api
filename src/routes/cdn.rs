//! `GET /cdn/{id}` — redirect to Drive Worker or Tidal
//! `POST /sync/enqueue` — enqueue album/playlist/track for bulk download

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    Json,
};
use base64::Engine;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::AppState;

/// GET /cdn/{id} — 302 to Drive Worker if cached, else to Tidal stream
pub async fn get_cdn(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    // 1. Check local CDN cache (Drive Worker)
    if let Some(pool) = &state.db {
        if let Some(link) = crate::cdn::get_cached_link(pool, &id).await {
            return Redirect::temporary(&link).into_response();
        }
        // Also try drive_file_id -> build worker URL if CDN_WORKER_URL set
        if let Some(file_id) = crate::cdn::get_cached_file_id(pool, &id).await {
            if let Some(worker) =
                crate::cdn::DriveWorkerClient::from_env(state.tidal_client.http_client().clone())
            {
                let url = worker.public_url(&file_id);
                return Redirect::temporary(&url).into_response();
            }
        }
    }

    // 2. Fallback: fetch Tidal manifest and redirect to first URL
    // This still goes through per-account limiter + anti-ban
    let track_id: i64 = match id.parse() {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid track id".to_string()).into_response(),
    };
    match fetch_tidal_stream_url(&state, track_id).await {
        Ok(url) => Redirect::temporary(&url).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn fetch_tidal_stream_url(state: &AppState, track_id: i64) -> Result<String, crate::error::AppError> {
    // Use the existing track manifest logic via tidal_client
    // For CDN we want LOSSLESS — reuse the same Tidal endpoint as /track/
    let url = format!("https://api.tidal.com/v1/tracks/{}/playbackinfopostpaywall", track_id);
    let v = state
        .tidal_client
        .make_request(
            &url,
            Some(vec![
                ("audioquality", "LOSSLESS"),
                ("playbackmode", "STREAM"),
                ("assetpresentation", "FULL"),
            ]),
        )
        .await?;
    // Try to extract manifest URL — if DASH, the URL is inside manifest, but for CDN we redirect to Tidal's playback URL
    // Simpler: return the manifest URL if present, else the API url
    if let Some(manifest) = v.get("data").and_then(|d| d.get("manifest")).and_then(|m| m.as_str()) {
        // manifest is base64 JSON with urls
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(manifest) {
            if let Ok(j) = serde_json::from_slice::<Value>(&decoded) {
                if let Some(urls) = j.get("urls").and_then(|u| u.as_array()) {
                    if let Some(first) = urls.first().and_then(|u| u.as_str()) {
                        return Ok(first.to_string());
                    }
                }
            }
        }
        // If not decodable, return manifest itself as redirect (client will handle)
        return Ok(format!("data:application/vnd.tidal.bts;base64,{}", manifest));
    }
    // Fallback to track manifest endpoint via tidal_client's make_request already handles auth
    Ok(format!("https://api.tidal.com/v1/tracks/{}/playbackinfopostpaywall?audioquality=LOSSLESS", track_id))
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct EnqueueReq {
    #[serde(default)]
    pub trackIds: Option<Vec<String>>,
    #[serde(default)]
    pub albumIds: Option<Vec<String>>,
    #[serde(default)]
    pub playlistIds: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct EnqueueResp {
    pub enqueued: usize,
    pub skipped: usize,
    pub bytes_today: i64,
}

/// POST /sync/enqueue — expand albums/playlists to trackIds and queue
pub async fn post_enqueue(
    State(state): State<AppState>,
    Json(req): Json<EnqueueReq>,
) -> Result<Json<EnqueueResp>, crate::error::AppError> {
    let pool = state.db.as_ref().ok_or_else(|| {
        crate::error::AppError::ServiceUnavailable("ephemeral mode — no DB for sync queue".into())
    })?;

    // Daily guard: 500 tracks soft limit (~50GB assuming 100MB avg), 750GB hard
    let bytes_today = crate::cdn::bytes_uploaded_today(pool).await;
    if bytes_today > 750 * 1024 * 1024 * 1024 {
        return Err(crate::error::AppError::ServiceUnavailable(
            "daily 750GB Drive limit reached — try tomorrow".into(),
        ));
    }
    // Count pending
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status IN ('pending','downloading','uploading')")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    if pending > 500 {
        return Err(crate::error::AppError::ServiceUnavailable(
            format!("too many pending jobs ({}) — wait for sync to drain", pending),
        ));
    }

    let mut enqueued = 0usize;
    let mut skipped = 0usize;

    if let Some(ids) = req.trackIds {
        for id in ids.into_iter().take(1000) {
            let tid = id.trim().to_string();
            if tid.is_empty() {
                continue;
            }
            match crate::cdn::enqueue_track(pool, &tid).await {
                Ok(true) => enqueued += 1,
                Ok(false) => skipped += 1,
                Err(e) => tracing::warn!("enqueue track {} failed: {}", tid, e),
            }
            // Anti-ban gap: 100ms between queue inserts (not Tidal)
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    if let Some(album_ids) = req.albumIds {
        for aid in album_ids.into_iter().take(100) {
            let aid = aid.trim().to_string();
            if aid.is_empty() {
                continue;
            }
            // Anti-ban gap: 800ms + jitter between album expands
            {
                let jitter = rand::thread_rng().gen_range(0..400);
                tokio::time::sleep(std::time::Duration::from_millis(800 + jitter)).await;
            }
            let data = state
                .tidal_client
                .make_request(
                    &format!("https://api.tidal.com/v1/albums/{}/items", aid),
                    Some(vec![("limit", "100"), ("offset", "0")]),
                )
                .await;
            match data {
                Ok(v) => {
                    let items = v.get("items").and_then(|x| x.as_array()).cloned().unwrap_or_default();
                    for item in items {
                        let tid = item
                            .get("item")
                            .and_then(|x| x.get("id"))
                            .or_else(|| item.get("id"))
                            .and_then(|x| x.as_i64().map(|n| n.to_string()).or_else(|| x.as_str().map(|s| s.to_string())))
                            .unwrap_or_default();
                        if tid.is_empty() { continue; }
                        match crate::cdn::enqueue_track(pool, &tid).await {
                            Ok(true) => enqueued += 1,
                            Ok(false) => skipped += 1,
                            Err(_) => {}
                        }
                    }
                }
                Err(e) => tracing::warn!("enqueue album {} fetch failed: {}", aid, e),
            }
        }
    }

    if let Some(playlist_ids) = req.playlistIds {
        for pid in playlist_ids.into_iter().take(100) {
            let pid = pid.trim().to_string();
            if pid.is_empty() { continue; }
            {
                let jitter = rand::thread_rng().gen_range(0..400);
                tokio::time::sleep(std::time::Duration::from_millis(800 + jitter)).await;
            }
            let data = state
                .tidal_client
                .make_request(
                    &format!("https://api.tidal.com/v1/playlists/{}/items", pid),
                    Some(vec![("limit", "100"), ("offset", "0")]),
                )
                .await;
            if let Ok(v) = data {
                let items = v.get("items").and_then(|x| x.as_array()).cloned().unwrap_or_default();
                for item in items {
                    let tid = item.get("item").and_then(|x| x.get("id")).and_then(|x| x.as_i64().map(|n| n.to_string())).unwrap_or_default().to_string();
                    if tid == "0" { continue; }
                    if tid.is_empty() { continue; }
                    match crate::cdn::enqueue_track(pool, &tid).await {
                        Ok(true) => enqueued += 1,
                        Ok(false) => skipped += 1,
                        Err(_) => {}
                    }
                }
            }
        }
    }

    Ok(Json(EnqueueResp {
        enqueued,
        skipped,
        bytes_today,
    }))
}

/// GET /sync/status — queue stats
pub async fn get_sync_status(State(state): State<AppState>) -> Result<Json<Value>, crate::error::AppError> {
    let pool = state.db.as_ref().ok_or_else(|| {
        crate::error::AppError::ServiceUnavailable("ephemeral mode — no DB".into())
    })?;
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='pending'").fetch_one(pool).await.unwrap_or(0);
    let downloading: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='downloading'").fetch_one(pool).await.unwrap_or(0);
    let uploading: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='uploading'").fetch_one(pool).await.unwrap_or(0);
    let done: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='done'").fetch_one(pool).await.unwrap_or(0);
    let failed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_jobs WHERE status='failed'").fetch_one(pool).await.unwrap_or(0);
    let cached: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cdn_cache").fetch_one(pool).await.unwrap_or(0);
    let bytes_today = crate::cdn::bytes_uploaded_today(pool).await;
    Ok(Json(json!({
        "pending": pending,
        "downloading": downloading,
        "uploading": uploading,
        "done": done,
        "failed": failed,
        "cached": cached,
        "bytes_today": bytes_today,
        "worker_configured": crate::cdn::DriveWorkerClient::from_env(state.tidal_client.http_client().clone()).is_some()
    })))
}
