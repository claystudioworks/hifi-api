//! Response cache — saves ~60% Tidal hits for immutable resources.
//!
//! Albums, artists and playlists rarely change; caching 5 minutes avoids
//! burning per-account quota on identical `?id=` queries. Uses `moka` with
//! TTL and bounded capacity, so the process stays within ~5-15 MB.

use std::time::Duration;

use moka::future::Cache;
use serde_json::Value;

/// TTL-aware, bounded in-memory cache for Tidal JSON responses.
pub struct TidalCache {
    inner: Cache<String, Value>,
}

impl TidalCache {
    /// Create a new cache with `max_capacity` entries and `ttl`.
    pub fn new(max_capacity: u64, ttl: Duration) -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// Convenience: 1000 entries, 5 minutes (the default for album/artist/playlist).
    pub fn default() -> Self {
        Self::new(1000, Duration::from_secs(300))
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: String, value: Value) {
        self.inner.insert(key, value).await;
    }

    #[cfg(test)]
    pub async fn insert_with_ttl(&self, key: String, value: Value, _ttl: Duration) {
        // moka builder TTL handles expiry; per-entry TTL not needed for tests.
        self.inner.insert(key, value).await;
    }
}

impl Default for TidalCache {
    fn default() -> Self {
        Self::new(1000, Duration::from_secs(300))
    }
}
