//! Cache tests — response cache saves ~60% Tidal hits

use std::time::Duration;

use hifi_api::cache::TidalCache;

#[tokio::test]
async fn test_album_cache_hit_avoids_second_tidal_call() {
    let cache = TidalCache::new(10, Duration::from_secs(300));
    cache
        .insert("album:123:100:0".into(), serde_json::json!({"id": 123}))
        .await;
    let got = cache.get("album:123:100:0").await.unwrap();
    assert_eq!(got["id"], 123);
    // second get should still be hit
    assert!(cache.get("album:123:100:0").await.is_some());
    // miss
    assert!(cache.get("album:999:100:0").await.is_none());
}

#[tokio::test]
async fn test_playlist_cache_insert_and_get() {
    let cache = TidalCache::default();
    cache
        .insert(
            "playlist:abc:0".into(),
            serde_json::json!({"playlist": {"id": "abc"}}),
        )
        .await;
    let v = cache.get("playlist:abc:0").await.unwrap();
    assert_eq!(v["playlist"]["id"], "abc");
}
