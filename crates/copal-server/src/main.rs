//! Copal server binary.

use copal_blob::FsBlobStore;
use copal_server::{build_router, AppState, Config};
use copal_store::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "copal_server=info,copal_store=info".into()),
        )
        .init();

    let config = Config::from_env();
    tracing::info!(bind = %config.bind, db = %config.store.url, "starting copal");

    let store = Store::connect(config.store.clone()).await?;
    let blobs = FsBlobStore::open(&config.blob_root)?;
    let mut state = AppState::new(store.clone(), blobs);
    state.limits = copal_server::app::Limits {
        max_upload_bytes: config.max_upload_bytes,
        upload_lease_secs: config.upload_lease_secs,
    };
    tracing::info!(instance = %state.instance_id, "upload-claim owner id");
    let router = build_router(state);

    // The reaper: expired upload claims (crashed instances, vanished
    // clients) sweep to `failed`, which is retryable.
    let reaper_store = store.clone();
    let interval = config.reaper_interval_secs.max(1);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
        loop {
            ticker.tick().await;
            match copal_store::repo::file::reap_expired_uploads(&reaper_store).await {
                Ok(reaped) if !reaped.is_empty() => {
                    tracing::info!(count = reaped.len(), "reaped expired upload claims");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "reaper sweep failed"),
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, router).await?;
    Ok(())
}
