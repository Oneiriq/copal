//! Copal server binary.

use copal_blob::ObjectStore;
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

    let engine_access =
        config
            .store
            .engine_access_key
            .as_ref()
            .map(|key| copal_server::engine::EngineAccess {
                key: key.clone(),
                namespace: config.store.namespace.clone(),
                database: config.store.database.clone(),
            });
    let store = Store::connect(config.store.clone()).await?;
    let open_blobs = || match &config.blob_encryption_key {
        Some(key) => ObjectStore::open_encrypted(&config.blob_root, key),
        None => ObjectStore::open(&config.blob_root),
    };
    let blobs = open_blobs()?;
    // Named residencies open beside local; the master key, when set,
    // seals content in every one of them.
    let build_residencies =
        || -> Result<copal_server::app::Residencies<ObjectStore>, Box<dyn std::error::Error>> {
            let mut named = std::collections::HashMap::new();
            for (name, backend_config) in &config.residencies {
                if !copal_store::repo::tenant::valid_residency_name(name) {
                    return Err(format!(
                        "residency name {name} is invalid: 1..=32 lowercase alphanumeric",
                    )
                    .into());
                }
                let mut backend = ObjectStore::open_backend(backend_config)?;
                if let Some(key) = &config.blob_encryption_key {
                    backend = backend.with_cipher(key)?;
                }
                named.insert(name.clone(), backend);
            }
            Ok(copal_server::app::Residencies {
                local: open_blobs()?,
                named,
            })
        };
    let residencies = build_residencies()?;
    if !residencies.named.is_empty() {
        tracing::info!(
            count = residencies.named.len(),
            "named storage residencies configured",
        );
    }
    let policy = match &config.blocked_extensions {
        Some(list) => copal_core::ExtensionPolicy::from_list(list),
        None => copal_core::ExtensionPolicy::standard(),
    };
    let registry = copal_server::pipeline::standard_registry(
        store.clone(),
        residencies.clone(),
        policy,
        config.enforce_type_match,
        config.clamav_addr.clone(),
        config.extractor_addr.clone(),
        config
            .embedding_addr
            .clone()
            .map(|addr| (addr, config.embedding_model.clone())),
    );
    if let Some(addr) = &config.embedding_addr {
        // The index has to exist at the model's width before the
        // first vector lands.
        store
            .ensure_vector_index(config.embedding_dimension)
            .await?;
        tracing::info!(
            service = %addr,
            model = %config.embedding_model,
            dimension = config.embedding_dimension,
            "semantic search enabled",
        );
    }
    if let Some(addr) = &config.clamav_addr {
        tracing::info!(clamd = %addr, "malware scanning enabled; content is withheld until scanned");
    }
    if config.auth.mode == copal_server::auth::AuthMode::TrustedHeader {
        tracing::warn!(
            "auth mode is TRUSTED HEADER (x-copal-tenant): development only; \
             set COPAL_AUTH_MODE=keys with COPAL_ADMIN_TOKEN for any exposed deployment",
        );
    }
    let mut state = AppState::new(store.clone(), blobs)
        .with_engine_access(engine_access)
        .with_flow(registry.clone())
        .with_auth(config.auth.clone())
        .with_residencies(residencies.named.clone())
        .with_scan_gate(config.clamav_addr.is_some())
        .with_cipher(match &config.blob_encryption_key {
            Some(key) => Some(copal_blob::crypto::BlobCipher::from_hex(key)?),
            None => None,
        })
        .with_embedding(
            config
                .embedding_addr
                .clone()
                .map(|addr| (addr, config.embedding_model.clone())),
        );
    state.limits = copal_server::app::Limits {
        max_upload_bytes: config.max_upload_bytes,
        upload_lease_secs: config.upload_lease_secs,
        request_timeout_secs: config.request_timeout_secs,
        transfer_timeout_secs: config.transfer_timeout_secs,
        tus_session_ttl_secs: u32::try_from(config.tus_session_ttl_secs).unwrap_or(86_400),
        max_semantic_distance: config.max_semantic_distance,
        subscription_max_secs: config.subscription_max_secs,
        min_multipart_part_bytes: copal_server::app::Limits::default().min_multipart_part_bytes,
        allow_private_webhook_targets: config.allow_private_webhook_targets,
    };
    if config.allow_private_webhook_targets {
        tracing::warn!(
            "webhook targets on private addresses are ALLOWED              (COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS): tenant-supplied URLs can reach              services inside this deployment",
        );
    }
    match config.rate_ledger.as_str() {
        "memory" => {}
        "store" => {
            // One budget for the whole fleet, kept where every
            // replica already looks.
            state.rate_store =
                std::sync::Arc::new(copal_server::rate::SurrealRateStore::new(store.clone()));
            tracing::info!("consumption ledger: shared store");
        }
        other => {
            return Err(
                format!("COPAL_RATE_LEDGER must be memory or store, not {other:?}",).into(),
            );
        }
    }
    if let Some(path) = &config.persisted_operations {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("COPAL_PERSISTED_OPERATIONS: {path}: {e}"))?;
        let listed: std::collections::HashMap<String, String> = serde_json::from_str(&raw)
            .map_err(|e| format!("COPAL_PERSISTED_OPERATIONS: {path}: {e}"))?;
        // Every hash must be the hash of its document, checked now: an
        // allowlist that lies refuses at startup rather than serving
        // the wrong operation later.
        for (hash, document) in &listed {
            let actual = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(document.as_bytes()));
            if &actual != hash {
                return Err(
                    format!("COPAL_PERSISTED_OPERATIONS: entry {hash} hashes to {actual}",).into(),
                );
            }
        }
        tracing::info!(
            operations = listed.len(),
            "graphql runs persisted operations only"
        );
        state.persisted_operations = Some(std::sync::Arc::new(listed));
    }
    tracing::info!(instance = %state.instance_id, "upload-claim owner id");
    let instance_id = state.instance_id.clone();
    let cors = config.cors_origins.as_deref();

    if state.cipher.is_none() {
        tracing::info!("sealed-secret surfaces are disabled: COPAL_BLOB_ENCRYPTION_KEY is unset",);
    }

    // The S3 gateway serves bucket routes at the root of its own
    // listener, so stock tooling needs only an endpoint URL. It exists
    // only with an encryption key: SigV4 verification reads the shared
    // secret back, and Copal stores such secrets sealed or not at all.
    if let Some(s3_bind) = &config.s3_bind {
        if state.cipher.is_none() {
            return Err(
                "COPAL_S3_BIND requires COPAL_BLOB_ENCRYPTION_KEY: gateway credentials \
                 are stored sealed under it"
                    .into(),
            );
        }
        let gateway = copal_server::s3::s3_router(state.clone());
        let listener = tokio::net::TcpListener::bind(s3_bind).await?;
        tracing::info!(addr = %listener.local_addr()?, "s3 gateway listening");
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, gateway).await {
                tracing::error!(error = %err, "s3 listener failed");
            }
        });
    }
    // Credential management joins the admin surface only when the
    // gateway is configured.
    let admin_extras = config
        .s3_bind
        .as_ref()
        .map(|_| copal_server::s3::s3_admin_router(state.clone()));

    // Webhooks and cg2 edge tokens store secrets sealed under the
    // master key, so their surfaces exist under the same gate.
    let sealed_surfaces = state.cipher.is_some();
    let webhook_routes =
        sealed_surfaces.then(|| copal_server::webhooks::webhook_router(state.clone()));
    let edge_routes = sealed_surfaces.then(|| copal_server::edge::edge_router(state.clone()));
    let edge_admin = sealed_surfaces.then(|| copal_server::edge::edge_admin_router(state.clone()));
    if let Some(cipher) = state.cipher.clone() {
        tokio::spawn(copal_server::webhooks::run_forever(
            store.clone(),
            cipher,
            instance_id.clone(),
            config.allow_private_webhook_targets,
        ));
    }

    // With a dedicated admin bind, the tenant listener never carries
    // admin routes at all; otherwise everything shares one router.
    let router = match &config.admin_bind {
        Some(admin_bind) => {
            let mut admin = copal_server::app::admin_router(state.clone());
            if let Some(extras) = admin_extras {
                admin = admin.merge(extras);
            }
            if let Some(extras) = edge_admin {
                admin = admin.merge(extras);
            }
            let listener = tokio::net::TcpListener::bind(admin_bind).await?;
            tracing::info!(addr = %listener.local_addr()?, "admin surface listening");
            tokio::spawn(async move {
                if let Err(err) = axum::serve(listener, admin).await {
                    tracing::error!(error = %err, "admin listener failed");
                }
            });
            let mut router = copal_server::app::api_router_with_cors(state, cors);
            if let Some(webhooks) = webhook_routes {
                router = router.merge(webhooks);
            }
            if let Some(edge) = edge_routes {
                router = router.merge(edge);
            }
            router
        }
        None => {
            let mut router = copal_server::app::build_router_with_cors(state, cors);
            if let Some(extras) = admin_extras {
                router = router.merge(extras);
            }
            if let Some(webhooks) = webhook_routes {
                router = router.merge(webhooks);
            }
            if let Some(edge) = edge_routes {
                router = router.merge(edge);
            }
            if let Some(extras) = edge_admin {
                router = router.merge(extras);
            }
            router
        }
    };

    // Maintenance: claim reaping, staging TTL, and content GC share one
    // interval loop; each sweep is failure-isolated inside it.
    tokio::spawn(copal_server::sweeps::run_forever(
        store.clone(),
        build_residencies()?,
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
