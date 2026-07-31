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

    // Maintenance: claim reaping, staging TTL, and content GC share one
    // interval loop; each sweep is failure-isolated inside it.
    tokio::spawn(copal_server::sweeps::run_forever(
        store.clone(),
        FsBlobStore::open(&config.blob_root)?,
        config.sweeps,
    ));

    // Durable execution worker: claims pending runs and executes them
    // over the journal. The production registry starts empty until the
    // processing activities land; the worker idles harmlessly.
    tokio::spawn(copal_flow::run_worker(
        copal_flow::FlowEngine::new(store.clone(), copal_flow::FlowRegistry::new()),
        format!(
            "worker-{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        ),
        2,
    ));

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, router).await?;
    Ok(())
}
