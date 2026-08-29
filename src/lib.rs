//! hifi-api library facade.
//!
//! Exposes the Axum router and application state so the API can be embedded
//! in another process (e.g. the sadda Tauri app) instead of running as a
//! standalone binary. `main.rs` is now a thin wrapper around this module.

pub mod account_manager;
pub mod admin;
pub mod anti_ban;
pub mod config;
pub mod db;
pub mod error;
pub mod ip_limiter;
pub mod proxy_manager;
pub mod rate_limit;
pub mod routes;
pub mod setup;
pub mod tidal_client;
pub mod token_manager;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::Method;
use axum::middleware;
use axum::routing::{any, get, patch, post, put};
use axum::{Json, Router};
use reqwest::Client;
use serde_json::Value;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::account_manager::{AccountManager, SwitchingWeights};
use crate::config::Config;
use crate::token_manager::TokenManager;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub account_manager: Arc<AccountManager>,
    pub token_manager: Arc<TokenManager>,
    pub tidal_client: Arc<tidal_client::TidalClient>,
    pub proxy_manager: Arc<proxy_manager::ProxyManager>,
    pub anti_ban: Arc<anti_ban::AntiBan>,
    pub rate_limits: Arc<rate_limit::RateLimitSettings>,
    pub db: Option<sqlx::SqlitePool>,
    pub setup_sessions: admin::setup::Sessions,
}

/// Build a shared HTTP client with the same tuning as the standalone binary.
pub fn build_http_client() -> Arc<Client> {
    let client = Client::builder()
        .gzip(true)
        .http2_prior_knowledge()
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(500)
        .pool_idle_timeout(Duration::from_secs(30))
        .user_agent("okhttp/5.3.2")
        .build()
        .expect("Failed to build HTTP client");
    Arc::new(client)
}

/// Construct the full application state from a [`Config`].
///
/// Mirrors the standalone binary's startup: DB init (with ephemeral
/// fallback), env-var account bootstrap, background limiter rebuild and
/// token pre-warming.
pub async fn build_state(config: Config, http_client: Option<Arc<Client>>) -> AppState {
    let config = Arc::new(config);

    let db = if config.database_url.is_empty() || config.database_url == "ephemeral" {
        tracing::info!("Running in ephemeral mode — no database");
        None
    } else {
        match db::init_pool(&config.database_url).await {
            Ok(pool) => {
                tracing::info!("Database initialized at {}", config.database_url);
                Some(pool)
            }
            Err(e) => {
                tracing::warn!("Failed to initialize database (ephemeral fallback): {}", e);
                None
            }
        }
    };

    let http_client = http_client.unwrap_or_else(build_http_client);

    let switching_weights = SwitchingWeights::default();
    let account_manager = Arc::new(AccountManager::new(db.clone(), switching_weights));

    if let Err(e) = account_manager.load_from_db().await {
        tracing::warn!("Could not load accounts from DB: {}", e);
    }

    if account_manager.account_count().await == 0 {
        let env_client_id = std::env::var("CLIENT_ID").unwrap_or_default();
        let env_client_secret = std::env::var("CLIENT_SECRET").unwrap_or_default();
        let env_refresh_token = std::env::var("REFRESH_TOKEN").unwrap_or_default();

        if !env_client_id.is_empty() && !env_refresh_token.is_empty() {
            let client_secret = if env_client_secret.is_empty() {
                "Y8tIpqKJxs9BEIwYr0I9bSbMWDsogXJx9LaN3mCHwD4%3D".to_string()
            } else {
                env_client_secret
            };
            let env_user_id = std::env::var("USER_ID").ok();
            match account_manager
                .add_account(
                    "Default Account (env)".into(),
                    env_client_id,
                    client_secret,
                    env_refresh_token,
                    env_user_id,
                )
                .await
            {
                Ok(acc) => tracing::info!("Loaded account from env vars ({})", acc.id),
                Err(e) => tracing::warn!("Failed to load account from env vars: {}", e),
            }
        }
    }

    if account_manager.account_count().await == 0 {
        if std::env::var("AUTO_SETUP").unwrap_or_default() == "true" {
            tracing::info!("AUTO_SETUP=true: Starting OAuth setup in background...");
            let am = account_manager.clone();
            let hc = http_client.clone();
            tokio::spawn(async move {
                if let Err(e) = setup::run_setup(&am, hc.as_ref()).await {
                    tracing::warn!(
                        "Auto-setup failed: {}. Add accounts via admin panel or env vars.",
                        e
                    );
                }
            });
        } else {
            tracing::warn!(
                "No Tidal accounts configured. Add one via the admin panel at /admin or set CLIENT_ID/REFRESH_TOKEN"
            );
        }
    }

    let token_manager = Arc::new(TokenManager::new(db.clone()));
    token_manager.set_account_manager(account_manager.clone());

    let rate_limits = Arc::new(rate_limit::RateLimitSettings::from_env());
    if let Some(db) = &db {
        rate_limits.load_from_db(db).await;
    }

    let anti_ban = Arc::new(anti_ban::AntiBan::new(rate_limits.clone()));

    // Periodically rebuild the per-IP limiter so stale IP buckets are dropped
    {
        let ab = anti_ban.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600));
            loop {
                interval.tick().await;
                ab.reload_limiter();
            }
        });
    }

    let tidal_client = Arc::new(tidal_client::TidalClient::new(
        (*http_client).clone(),
        token_manager.clone(),
        account_manager.clone(),
        rate_limits.clone(),
        config.clone(),
    ));

    let proxy_manager = Arc::new(proxy_manager::ProxyManager::new(config.clone()));

    token_manager
        .clone()
        .start_prewarm_loop(account_manager.clone(), http_client)
        .await;

    AppState {
        config,
        account_manager,
        token_manager,
        tidal_client,
        proxy_manager,
        anti_ban,
        rate_limits,
        db,
        setup_sessions: admin::setup::new_session_store(),
    }
}

/// Build the full Axum router (public API + admin panel + middleware).
pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(Any);

    Router::new()
        // Public API routes
        .route("/", get(index))
        .route("/info/", get(routes::info::get_info))
        .route("/track/", get(routes::track::get_track))
        .route("/trackManifests/{id}", get(routes::track::get_track_manifests))
        // Legacy query-param style used by binimum/hifi-api clients:
        // /trackManifests/?id={id}&quality={...}
        .route("/trackManifests", get(routes::track::get_track_manifests_legacy))
        .route("/trackManifests/", get(routes::track::get_track_manifests_legacy))
        .route("/dash/{id}", get(routes::track::get_dash_stream))
        .route("/stream/{id}", get(routes::track::get_stream))
        .route("/widevine", any(routes::widevine::widevine_proxy))
        .route("/recommendations/", get(routes::recommendations::get_recommendations))
        .route("/search/", get(routes::search::search))
        .route("/album/", get(routes::album::get_album))
        .route("/album/similar/", get(routes::similar_albums::get_similar_albums))
        .route("/artist/", get(routes::artist::get_artist))
        .route("/artist/similar/", get(routes::similar_artists::get_similar_artists))
        .route("/mix/", get(routes::mix::get_mix))
        .route("/playlist/", get(routes::playlist::get_playlist))
        .route("/cover/", get(routes::cover::get_cover))
        .route("/lyrics/", get(routes::lyrics::get_lyrics))
        .route("/topvideos/", get(routes::topvideos::get_top_videos))
        .route("/video/", get(routes::video::get_video))
        .route("/health", get(routes::health::health))
        // Admin SPA (no auth — the SPA handles auth in-browser)
        .route("/admin", get(crate::admin::ui::admin_index))
        // Admin API routes (auth-protected)
        .nest("/admin", admin_api(state.clone()))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            ip_limiter::enforce_ip_rate_limit,
        ))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn admin_api(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/accounts", get(crate::admin::accounts::list_accounts).post(crate::admin::accounts::add_account))
        .route("/accounts/{id}", patch(crate::admin::accounts::update_account).delete(crate::admin::accounts::remove_account))
        .route("/accounts/{id}/toggle", put(crate::admin::accounts::toggle_account))
        .route("/accounts/test-all", post(crate::admin::accounts::test_all_accounts))
        .route("/accounts/{id}/test", post(crate::admin::accounts::test_account))
        .route("/accounts/{id}/refresh", post(crate::admin::accounts::refresh_account_token))
        .route("/stats", get(crate::admin::stats::get_stats))
        .route(
            "/settings",
            get(crate::admin::settings::get_settings).put(crate::admin::settings::update_settings),
        )
        .route("/setup", post(crate::admin::setup::start_setup))
        .route("/setup/{session}", get(crate::admin::setup::check_setup))
        .layer(middleware::from_fn_with_state(state, crate::admin::admin_auth))
}

async fn index(State(state): State<AppState>) -> Json<Value> {
    routes::index(&state.config)
}

/// Bind the router on `config.host:config.port` and spawn serving in the
/// background. Returns the bound address (useful when port == 0 for an
/// ephemeral port).
pub async fn serve(state: AppState) -> std::io::Result<SocketAddr> {
    let addr = format!("{}:{}", state.config.host, state.config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let local = listener.local_addr()?;
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("hifi-api server error: {}", e);
        }
    });
    Ok(local)
}
