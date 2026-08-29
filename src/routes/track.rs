use std::sync::{Arc, LazyLock};

use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use base64::Engine;
use moka::future::Cache;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::AppState;

fn de_comma_list<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    Ok(s.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

#[derive(Deserialize)]
pub struct TrackParams {
    pub id: i64,
    #[serde(default = "default_quality")]
    pub quality: String,
    #[serde(default)]
    pub immersiveaudio: bool,
}

fn default_quality() -> String {
    "HI_RES_LOSSLESS".to_string()
}

pub async fn get_track(
    State(state): State<AppState>,
    Query(params): Query<TrackParams>,
) -> Result<Json<Value>, AppError> {
    let url = format!("https://api.tidal.com/v1/tracks/{}/playbackinfo", params.id);
    let result = state
        .tidal_client
        .make_request(
            &url,
            Some(vec![
                ("audioquality", &params.quality),
                ("playbackmode", "STREAM"),
                ("assetpresentation", "FULL"),
                ("immersiveaudio", if params.immersiveaudio { "true" } else { "false" }),
            ]),
        )
        .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct TrackManifestsParams {
    #[serde(default = "default_formats", deserialize_with = "de_comma_list")]
    pub formats: Vec<String>,
    #[serde(default = "default_adaptive")]
    pub adaptive: String,
    #[serde(default = "default_manifest_type")]
    pub manifestType: String,
    #[serde(default = "default_uri_scheme")]
    pub uriScheme: String,
    #[serde(default = "default_usage")]
    pub usage: String,
}

fn default_formats() -> Vec<String> {
    vec![
        "HEAACV1".into(),
        "AACLC".into(),
        "FLAC".into(),
        "FLAC_HIRES".into(),
        "EAC3_JOC".into(),
    ]
}
fn default_adaptive() -> String {
    "true".into()
}
fn default_manifest_type() -> String {
    "MPEG_DASH".into()
}
fn default_uri_scheme() -> String {
    "HTTPS".into()
}
fn default_usage() -> String {
    "PLAYBACK".into()
}

pub async fn get_track_manifests(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
    Query(params): Query<TrackManifestsParams>,
    req: Request,
) -> Result<Json<Value>, AppError> {
    get_manifests_inner(
        state,
        track_id,
        params.formats,
        params.adaptive,
        params.manifestType,
        params.uriScheme,
        params.usage,
        req,
    )
    .await
}

/// Legacy-compat handler matching binimum/hifi-api's query-param style:
/// GET /trackManifests/?id={id}&quality={LOSSLESS|HI_RES_LOSSLESS|HIGH|LOW}
/// Used by sadda's hifi_engine.rs fallback path.
#[derive(Deserialize)]
pub struct LegacyManifestParams {
    pub id: String,
    pub quality: Option<String>,
}

pub fn quality_to_formats(quality: Option<&str>) -> Vec<String> {
    match quality.unwrap_or("") {
        "LOW" | "HIGH" => vec!["HEAACV1".into(), "AACLC".into()],
        "LOSSLESS" => vec!["HEAACV1".into(), "AACLC".into(), "FLAC".into()],
        _ => vec![
            "HEAACV1".into(),
            "AACLC".into(),
            "FLAC".into(),
            "FLAC_HIRES".into(),
            "EAC3_JOC".into(),
        ],
    }
}

pub async fn get_track_manifests_legacy(
    State(state): State<AppState>,
    Query(params): Query<LegacyManifestParams>,
    req: Request,
) -> Result<Json<Value>, AppError> {
    get_manifests_inner(
        state,
        params.id,
        quality_to_formats(params.quality.as_deref()),
        "true".into(),
        "MPEG_DASH".into(),
        "HTTPS".into(),
        "PLAYBACK".into(),
        req,
    )
    .await
}

async fn get_manifests_inner(
    state: AppState,
    track_id: String,
    formats: Vec<String>,
    adaptive: String,
    manifest_type: String,
    uri_scheme: String,
    usage: String,
    req: Request,
) -> Result<Json<Value>, AppError> {
    let url = format!(
        "https://openapi.tidal.com/v2/trackManifests/{}",
        track_id
    );

    let mut all_params: Vec<(&str, &str)> = vec![
        ("adaptive", adaptive.as_str()),
        ("manifestType", manifest_type.as_str()),
        ("uriScheme", uri_scheme.as_str()),
        ("usage", usage.as_str()),
    ];

    for fmt in &formats {
        all_params.push(("formats", fmt.as_str()));
    }

    let result = state
        .tidal_client
        .make_request(&url, Some(all_params))
        .await?;

    let mut result = result;
    if let Some(data) = result.get_mut("data") {
        if let Some(data_obj) = data.as_object_mut() {
            if let Some(data_inner) = data_obj.get_mut("data") {
                if let Some(attributes) = data_inner.get("attributes") {
                    if let Some(drm_data) = attributes.get("drmData") {
                        if let Some(_drm_obj) = drm_data.as_object() {
                            let proxy_url = format!(
                                "{}/widevine",
                                req.uri().authority().map(|a| {
                                    format!("{}://{}", 
                                        if req.uri().scheme_str() == Some("https") { "https" } else { "http" },
                                        a
                                    )
                                }).unwrap_or_default().trim_end_matches('/')
                            );
                            if let Some(drm) = data_inner.as_object_mut() {
                                if let Some(attrs) = drm.get_mut("attributes") {
                                    if let Some(attrs_obj) = attrs.as_object_mut() {
                                        if let Some(drm) = attrs_obj.get_mut("drmData") {
                                            if let Some(drm_obj) = drm.as_object_mut() {
                                                drm_obj.insert("licenseUrl".into(), json!(proxy_url.clone()));
                                                drm_obj.insert("certificateUrl".into(), json!(proxy_url));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(Json(result))
}

// ── Progressive stream assembly ────────────────────────────────────────────
//
// Tidal now serves MPEG-DASH manifests (base64 MPD XML) instead of direct
// BTS file URLs. A DASH "initialization" segment is only a few hundred
// bytes, so naive size probes misclassify full FLAC streams as preview
// clips. This endpoint resolves a track into ONE progressive audio
// response the app can play directly:
//
//   * BTS manifests  -> 302 redirect to the underlying file URL
//   * MPEG-DASH      -> init + all media segments downloaded server-side
//                       and concatenated into a single fMP4 stream with
//                       HTTP Range support.

/// Assembled DASH streams cached in memory (bounded by total bytes).
type DashCache = Cache<String, Arc<Vec<u8>>>;

static DASH_CACHE: LazyLock<DashCache> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(256 * 1024 * 1024) // 256 MB across assembled tracks
        .weigher(|_k: &String, v: &Arc<Vec<u8>>| v.len().max(1) as u32)
        .build()
});

fn b64_decode(data: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(data).ok()
}

/// Extracts `attr="value"` for the first occurrence of `attr` in the XML.
fn xml_attr<'a>(xml: &'a str, attr: &str) -> Option<&'a str> {
    let needle = format!("{}=\"", attr);
    let pos = xml.find(&needle)? + needle.len();
    let rest = &xml[pos..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn unescape_xml_url(u: &str) -> String {
    u.replace("&amp;", "&")
}

/// Parses an ISO-8601 duration like `PT5M29.359S` into seconds.
fn parse_iso8601_duration(s: &str) -> Option<f64> {
    let s = s.strip_prefix('P')?;
    let mut total = 0f64;
    let mut num = String::new();
    let mut in_time = false;
    for ch in s.chars() {
        match ch {
            'T' => in_time = true,
            '0'..='9' | '.' => num.push(ch),
            'H' if in_time => { total += num.parse::<f64>().unwrap_or(0.0) * 3600.0; num.clear(); }
            'M' if in_time => { total += num.parse::<f64>().unwrap_or(0.0) * 60.0; num.clear(); }
            'S' if in_time => { total += num.parse::<f64>().unwrap_or(0.0); num.clear(); }
            'D' => { total += num.parse::<f64>().unwrap_or(0.0) * 86400.0; num.clear(); }
            _ => {}
        }
    }
    Some(total)
}

/// Counts media segments from SegmentTemplate/SegmentTimeline metadata.
fn dash_segment_count(xml: &str, timescale: u64) -> Option<(u64, u64)> {
    let start_number: u64 = xml_attr(xml, "startNumber")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    // Preferred: explicit SegmentTimeline (<S t=".." d=".." r="..")
    if let Some(tl_start) = xml.find("<SegmentTimeline") {
        let tl_end = xml[tl_start..].find("</SegmentTimeline>").map(|e| tl_start + e).unwrap_or(xml.len());
        let timeline = &xml[tl_start..tl_end];
        let mut count = 0u64;
        for piece in timeline.split("<S ").skip(1) {
            let tag = piece.split('>').next().unwrap_or("");
            let d = xml_attr(tag, "d").and_then(|v| v.parse::<u64>().ok());
            let r: i64 = xml_attr(tag, "r").and_then(|v| v.parse().ok()).unwrap_or(0);
            match (d, r) {
                (_, -1) | (None, _) => break,
                (Some(_), r) => count += (r + 1).max(0) as u64,
            }
        }
        if count > 0 {
            return Some((start_number, count));
        }
    }

    // Fallback: fixed-duration segments + mediaPresentationDuration
    let seg_dur: u64 = xml_attr(xml, "duration").and_then(|v| v.parse().ok())?;
    if seg_dur == 0 || timescale == 0 {
        return None;
    }
    let total_secs = xml_attr(xml, "mediaPresentationDuration")
        .and_then(parse_iso8601_duration)?;
    let count = ((total_secs * timescale as f64) / seg_dur as f64).ceil() as u64;
    Some((start_number, count))
}

async fn assemble_dash(manifest_b64: &str) -> Result<Arc<Vec<u8>>, AppError> {
    let decoded = b64_decode(manifest_b64)
        .ok_or_else(|| AppError::Internal("Invalid base64 manifest".into()))?;
    let xml = String::from_utf8(decoded)
        .map_err(|_| AppError::Internal("Manifest is not UTF-8".into()))?;

    let init_url = xml_attr(&xml, "initialization")
        .filter(|u| u.starts_with("http"))
        .map(unescape_xml_url)
        .ok_or_else(|| AppError::NotFound("No initialization URL in MPD".into()))?;
    let media_template = xml_attr(&xml, "media")
        .filter(|u| u.contains("$Number"))
        .map(unescape_xml_url)
        .ok_or_else(|| AppError::NotFound("No numbered media template in MPD".into()))?;

    let timescale: u64 = xml_attr(&xml, "timescale")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let (start_number, count) = dash_segment_count(&xml, timescale)
        .ok_or_else(|| AppError::Internal("Could not determine DASH segment count".into()))?;
    if count == 0 || count > 3000 {
        return Err(AppError::Internal(format!("Unreasonable segment count: {}", count)));
    }

    // Cache key: init URLs are unique per track + quality + token set.
    let cache_key = init_url.clone();
    if let Some(hit) = DASH_CACHE.get(&cache_key).await {
        return Ok(hit);
    }

    let client = crate::build_http_client();
    let mut body: Vec<u8> = Vec::with_capacity(16 * 1024 * 1024);

    let init_resp = client
        .get(&init_url)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| AppError::UpstreamError(StatusCode::BAD_GATEWAY, format!("Init fetch failed: {e}")))?;
    if !init_resp.status().is_success() {
        return Err(AppError::UpstreamError(
            StatusCode::BAD_GATEWAY,
            format!("Init segment returned {}", init_resp.status()),
        ));
    }
    body.extend_from_slice(&init_resp.bytes().await.map_err(|e| {
        AppError::UpstreamError(StatusCode::BAD_GATEWAY, format!("Init read failed: {e}"))
    })?);

    for i in 0..count {
        let url = media_template.replace("$Number$", &(start_number + i).to_string());
        let resp = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| AppError::UpstreamError(StatusCode::BAD_GATEWAY, format!("Segment {} fetch failed: {e}", start_number + i)))?;
        if !resp.status().is_success() {
            return Err(AppError::UpstreamError(
                StatusCode::BAD_GATEWAY,
                format!("Segment {} returned {}", start_number + i, resp.status()),
            ));
        }
        let bytes = resp.bytes().await.map_err(|e| {
            AppError::UpstreamError(StatusCode::BAD_GATEWAY, format!("Segment {} read failed: {e}", start_number + i))
        })?;
        body.extend_from_slice(&bytes);
    }

    let assembled = Arc::new(body);
    DASH_CACHE.insert(cache_key, assembled.clone()).await;
    Ok(assembled)
}

/// Serves an assembled track with HTTP Range support so seeking works.
fn serve_bytes(bytes: &[u8], range_header: Option<&str>) -> Response {
    let total = bytes.len() as u64;
    let content_type = header::HeaderValue::from_static("audio/mp4");

    if let Some(range) = range_header.and_then(|r| r.strip_prefix("bytes=")) {
        let (start, end) = match range.split_once('-') {
            Some((s, "")) if !s.is_empty() => {
                let start: u64 = s.parse().unwrap_or(0);
                (start.min(total.saturating_sub(1)), total.saturating_sub(1))
            }
            Some((s, e)) if !s.is_empty() => {
                let start: u64 = s.parse().unwrap_or(0);
                let end: u64 = e.parse().unwrap_or(total - 1);
                (start.min(total.saturating_sub(1)), end.min(total.saturating_sub(1)))
            }
            Some(("", s)) => {
                // suffix range: last N bytes
                let n: u64 = s.parse().unwrap_or(0);
                (total.saturating_sub(n.min(total)), total.saturating_sub(1))
            }
            _ => (0, total.saturating_sub(1)),
        };
        if start <= end && total > 0 {
            let slice = &bytes[start as usize..=(end as usize)];
            return (
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::ACCEPT_RANGES, header::HeaderValue::from_static("bytes")),
                    (header::CONTENT_RANGE, header::HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, total)).unwrap()),
                    (header::CONTENT_LENGTH, header::HeaderValue::from_str(&slice.len().to_string()).unwrap()),
                ],
                slice.to_vec(),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::ACCEPT_RANGES, header::HeaderValue::from_static("bytes")),
            (header::CONTENT_LENGTH, header::HeaderValue::from_str(&total.to_string()).unwrap()),
        ],
        bytes.to_vec(),
    )
        .into_response()
}

/// GET /stream/{id}?quality={LOSSLESS|HI_RES_LOSSLESS|HIGH|LOW}
/// Single progressive audio response for any Tidal track.
pub async fn get_stream(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
    Query(params): Query<TrackParams>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let id: i64 = track_id.parse().map_err(|_| AppError::BadRequest("Bad track id".into()))?;
    let url = format!("https://api.tidal.com/v1/tracks/{}/playbackinfo", id);
    let result = state
        .tidal_client
        .make_request(
            &url,
            Some(vec![
                ("audioquality", params.quality.as_str()),
                ("playbackmode", "STREAM"),
                ("assetpresentation", "FULL"),
                ("immersiveaudio", if params.immersiveaudio { "true" } else { "false" }),
            ]),
        )
        .await?;

    let data = result.get("data").unwrap_or(&result);
    let mime = data
        .get("manifestMimeType")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let manifest_b64 = data
        .get("manifest")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::NotFound("No manifest in playbackinfo".into()))?;

    if mime.contains("dash") {
        let bytes = assemble_dash(manifest_b64).await?;
        let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
        return Ok(serve_bytes(&bytes, range));
    }

    // Legacy BTS manifest: base64 JSON containing direct file URL(s).
    if let Some(decoded) = b64_decode(manifest_b64) {
        if let Ok(manifest_json) = serde_json::from_slice::<Value>(&decoded) {
            if let Some(u) = manifest_json
                .pointer("/urls/0")
                .or_else(|| manifest_json.get("url"))
                .and_then(|v| v.as_str())
            {
                return Ok(Redirect::temporary(u).into_response());
            }
        }
    }

    Err(AppError::Internal(format!("Unsupported manifest type: {}", mime)))
}

pub async fn get_dash_stream(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
) -> Result<Redirect, AppError> {
    let url = format!("https://openapi.tidal.com/v2/trackManifests/{}", track_id);

    let all_params: Vec<(&str, &str)> = vec![
        ("adaptive", "true"),
        ("manifestType", "MPEG_DASH"),
        ("uriScheme", "HTTPS"),
        ("usage", "PLAYBACK"),
        ("formats", "FLAC_HIRES,FLAC,EAC3_JOC,AACLC"),
    ];

    let result = state
        .tidal_client
        .make_request(&url, Some(all_params))
        .await?;

    let uri = result
        .pointer("/data/data/attributes/uri")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal("No manifest URI in response".into()))?;

    Ok(Redirect::temporary(uri))
}
