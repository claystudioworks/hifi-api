//! Integration test: the Prometheus metrics endpoint.
//!
//! The metrics hangar is the "visibility before ban" layer: `GET /metrics`
//! must always expose at least the request/429 counters, so an operator can
//! see Tidal rate limiting start before the anti-ban layer goes to work.

use std::net::SocketAddr;

use hifi_api::{build_router, build_state, config::Config};

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus() {
    let state = build_state(
        Config::custom(
            "ephemeral".into(),
            "".into(),
            "US".into(),
            "127.0.0.1".into(),
            0,
        ),
        None,
    )
    .await;
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    });

    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("hifi_requests_total"),
        "/metrics should expose hifi_requests_total, got:\n{body}"
    );
    assert!(
        body.contains("hifi_429_total"),
        "/metrics should expose hifi_429_total, got:\n{body}"
    );
}