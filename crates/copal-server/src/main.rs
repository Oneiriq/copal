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
    let router = build_router(AppState { store, blobs });

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, router).await?;
    Ok(())
}
