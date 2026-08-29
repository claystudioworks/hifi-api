//! `GET /metrics` — Prometheus text exposition endpoint.
//!
//! Serves the process-wide registry as `text/plain; version=0.0.4` so a
//! Prometheus scraper (or a human curled at the port) can see request and
//! 429 counters before the anti-ban layer ever kicks in.

use axum::http::header;
use axum::response::IntoResponse;

/// Render all metrics in Prometheus text exposition format.
pub async fn get_metrics() -> impl IntoResponse {
    // Idempotent no-op after the first scrape: eagerly registers the static
    // counters so they show up in the output even when still at zero.
    crate::metrics::init();
    let body = crate::metrics::gather();
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}