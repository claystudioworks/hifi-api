//! CDN + sync queue tests

use std::net::SocketAddr;

use hifi_api::{build_router, build_state, config::Config};
use serde_json::json;

#[tokio::test]
async fn test_sync_enqueue_requires_db() {
    // ephemeral — no DB, should 503
    let state = build_state(
        Config::custom("ephemeral".into(), "".into(), "US".into(), "127.0.0.1".into(), 0),
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
        .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/sync/enqueue", addr))
        .json(&json!({"trackIds": ["123"]}))
        .send()
        .await
        .unwrap();
    // ephemeral mode — should be 503
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn test_sync_enqueue_and_status_with_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_cdn.db");
    // sqlx on Windows needs forward slashes after sqlite://
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        db_path.display().to_string().replace('\\', "/")
    );
    let cfg = Config::custom(db_url.clone(), "".into(), "US".into(), "127.0.0.1".into(), 0);
    // Use file DB so cdn_cache and sync_jobs exist
    let state = build_state(cfg, None).await;
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let client = reqwest::Client::new();

    // Enqueue 2 tracks
    let resp = client
        .post(format!("http://{}/sync/enqueue", addr))
        .json(&json!({"trackIds": ["111", "222"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["enqueued"], 2);

    // Duplicate should be skipped
    let resp2 = client
        .post(format!("http://{}/sync/enqueue", addr))
        .json(&json!({"trackIds": ["111"]}))
        .send()
        .await
        .unwrap();
    let body2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body2["enqueued"], 0);
    assert_eq!(body2["skipped"], 1);

    // Status should show pending 2
    let resp3 = client
        .get(format!("http://{}/sync/status", addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp3.status(), 200);
    let body3: serde_json::Value = resp3.json().await.unwrap();
    assert_eq!(body3["pending"], 2);
    assert_eq!(body3["cached"], 0);
}

#[tokio::test]
async fn test_cdn_redirect_miss_fallback() {
    // With no cache and no credentials, /cdn/{id} should 404 or 302 to Tidal fallback
    let state = build_state(
        Config::custom("ephemeral".into(), "".into(), "US".into(), "127.0.0.1".into(), 0),
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
        .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let resp = client
        .get(format!("http://{}/cdn/123", addr))
        .send()
        .await
        .unwrap();
    // Without DB cache and without Tidal creds, should be 404 (Tidal fallback fails)
    assert!(resp.status() == 404 || resp.status().is_redirection());
}
