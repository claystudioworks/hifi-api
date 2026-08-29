use std::net::SocketAddr;

use hifi_api::config::Config;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env();
    let addr = format!("{}:{}", config.host, config.port);
    let state = hifi_api::build_state(config, None).await;
    let bound = match hifi_api::serve(state).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Failed to bind {addr}: {e}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        "HiFi API v{} listening on {} (requested {addr})",
        "2.10",
        bound
    );

    // Keep the runtime alive; `serve` spawned the server as a background task.
    // axum::serve owns the listener inside the task, so park here.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

// Silence unused import warnings for SocketAddr in release builds where it is
// only used in type position above.
#[allow(dead_code)]
fn _assert(_: SocketAddr) {}
