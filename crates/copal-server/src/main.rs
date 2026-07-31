//! Copal server binary.

use copal_blob::FsBlobStore;
use copal_server::{AppState, Config};
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
    let open_blobs = || match &config.blob_encryption_key {
        Some(key) => FsBlobStore::open_encrypted(&config.blob_root, key),
        None => FsBlobStore::open(&config.blob_root),
    };
    let blobs = open_blobs()?;
    let policy = match &config.blocked_extensions {
        Some(list) => copal_core::ExtensionPolicy::from_list(list),
        None => copal_core::ExtensionPolicy::standard(),
    };
    let registry = copal_server::pipeline::standard_registry(
        store.clone(),
        blobs.clone(),
        policy,
        config.enforce_type_match,
    );
    if config.auth.mode == copal_server::auth::AuthMode::TrustedHeader {
        tracing::warn!(
            "auth mode is TRUSTED HEADER (x-copal-tenant): development only; \
             set COPAL_AUTH_MODE=keys with COPAL_ADMIN_TOKEN for any exposed deployment",
        );
    }
    let mut state = AppState::new(store.clone(), blobs)
        .with_flow(registry.clone())
        .with_auth(config.auth.clone());
    state.limits = copal_server::app::Limits {
        max_upload_bytes: config.max_upload_bytes,
        upload_lease_secs: config.upload_lease_secs,
        request_timeout_secs: config.request_timeout_secs,
        transfer_timeout_secs: config.transfer_timeout_secs,
    };
    tracing::info!(instance = %state.instance_id, "upload-claim owner id");
    let instance_id = state.instance_id.clone();
    let cors = config.cors_origins.as_deref();
    // With a dedicated admin bind, the tenant listener never carries
    // admin routes at all; otherwise everything shares one router.
    let router = match &config.admin_bind {
        Some(admin_bind) => {
            let admin = copal_server::app::admin_router(state.clone());
            let listener = tokio::net::TcpListener::bind(admin_bind).await?;
            tracing::info!(addr = %listener.local_addr()?, "admin surface listening");
            tokio::spawn(async move {
                if let Err(err) = axum::serve(listener, admin).await {
                    tracing::error!(error = %err, "admin listener failed");
                }
            });
            copal_server::app::api_router_with_cors(state, cors)
        }
        None => copal_server::app::build_router_with_cors(state, cors),
    };

    // Maintenance: claim reaping, staging TTL, and content GC share one
    // interval loop; each sweep is failure-isolated inside it.
    tokio::spawn(copal_server::sweeps::run_forever(
        store.clone(),
        open_blobs()?,
        config.sweeps,
        instance_id,
    ));

    // Durable execution worker: claims pending runs and executes them
    // over the journal. The production registry starts empty until the
    // processing activities land; the worker idles harmlessly.
    tokio::spawn(copal_flow::run_worker(
        copal_flow::FlowEngine::new(store.clone(), registry),
        format!(
            "worker-{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        ),
        2,
    ));

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    // Drain in-flight requests on SIGTERM or ctrl-c. Background tasks
    // stop with the process; their leases make that safe.
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("drained; goodbye");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received; draining");
}
