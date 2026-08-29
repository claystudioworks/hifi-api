//! Batch endpoint — bulk fetch without N+1

use std::net::SocketAddr;

use hifi_api::{build_router, build_state, config::Config};
use serde_json::json;

#[tokio::test]
async fn test_batch_returns_tracks_even_without_credentials() {
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
    // Give server a moment
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/batch", addr))
        .json(&json!({"trackIds": [427520487]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Should have tracks array with 1 element (error per track if no credentials)
    assert!(body["tracks"].is_array());
    assert_eq!(body["tracks"].as_array().unwrap().len(), 1);
    // Empty request should return empty arrays
    let resp2 = client
        .post(format!("http://{}/batch", addr))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    let body2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body2["tracks"].as_array().unwrap().len(), 0);
}
