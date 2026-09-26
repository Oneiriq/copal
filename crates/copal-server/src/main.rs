//! Copal server binary.

use copal_blob::ObjectStore;
use copal_server::{AppState, Config};
use copal_store::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(std::env::var("COPAL_OTLP_ENDPOINT").ok().as_deref());

    let mut config = Config::from_env();
    // Custody answers before any store opens, because every backend
    // below takes its key from the resolved configuration.
    if let Some(addr) = config.kms_addr.clone() {
        let material =
            copal_server::kms::fetch(&addr, &config.kms_key_id, config.kms_token.as_deref())
                .await?;
        if config.blob_encryption_key.is_some() {
            tracing::warn!(
                "COPAL_BLOB_ENCRYPTION_KEY is set and ignored: key custody at \
                 COPAL_KMS_ADDR is the authority",
            );
        }
        tracing::info!(
            addr = %addr,
            key = %config.kms_key_id,
            rotating = material.previous.is_some(),
            "master key taken from custody",
        );
        config.blob_encryption_key = Some(material.current);
        config.blob_encryption_key_previous = material.previous;
    }
    let config = config;
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
    let mut store_config = config.store.clone();
    // One declaration set, two enforcement layers: the engine policy
    // is derived from the contract, and a guard without an engine
    // clause refuses the boot here.
    store_config.engine_policy = copal_server::engine::engine_policy()?;
    let store = Store::connect(store_config).await?;
    let open_blobs = || {
        let store = match &config.blob_encryption_key {
            Some(key) => ObjectStore::open_encrypted(&config.blob_root, key),
            None => ObjectStore::open(&config.blob_root),
        }?;
        match &config.blob_encryption_key_previous {
            Some(previous) => store.with_previous_cipher(previous),
            None => Ok(store),
        }
    };
    let blobs = open_blobs()?;
    // Named residencies open beside local; the master key, when set,
    // seals content in every one of them.
    // A tier backend opens under its RESIDENCY's cipher: the
    // residency's own key when it carries one, the deployment master
    // otherwise. Hot and cold copies of one digest are the same
    // sealed object, so the tier must open exactly what the primary
    // wrote.
    let open_tiers = |tiers: &std::collections::HashMap<String, copal_blob::tier::TierConfig>,
                      residency_key: Option<&str>,
                      residency_previous: Option<&str>|
     -> Result<
        std::collections::HashMap<String, ObjectStore>,
        Box<dyn std::error::Error>,
    > {
        let mut opened = std::collections::HashMap::new();
        for (name, tier) in tiers {
            let mut backend = ObjectStore::open_backend(&tier.backend)?;
            if let Some(key) = residency_key.or(config.blob_encryption_key.as_deref()) {
                backend = backend.with_cipher(key)?;
            }
            if let Some(previous) =
                residency_previous.or(config.blob_encryption_key_previous.as_deref())
            {
                backend = backend.with_previous_cipher(previous)?;
            }
            opened.insert(name.clone(), backend);
        }
        Ok(opened)
    };
    let build_residencies =
        || -> Result<copal_server::app::Residencies<ObjectStore>, Box<dyn std::error::Error>> {
            let mut named = std::collections::HashMap::new();
            let mut tiers = std::collections::HashMap::new();
            for (name, residency_config) in &config.residencies {
                if !copal_store::repo::tenant::valid_residency_name(name) {
                    return Err(format!(
                        "residency name {name} is invalid: 1..=32 lowercase alphanumeric",
                    )
                    .into());
                }
                let mut backend = ObjectStore::open_backend(&residency_config.backend)?;
                if let Some(key) = &config.blob_encryption_key {
                    backend = backend.with_cipher(key)?;
                }
                if let Some(previous) = &config.blob_encryption_key_previous {
                    backend = backend.with_previous_cipher(previous)?;
                }
                named.insert(name.clone(), backend);
                if !residency_config.tiers.is_empty() {
                    tiers.insert(
                        name.clone(),
                        open_tiers(
                            &residency_config.tiers,
                            residency_config.backend.encryption_key(),
                            residency_config.backend.previous_encryption_key(),
                        )?,
                    );
                }
            }
            if !config.local_tiers.is_empty() {
                tiers.insert(
                    "local".to_owned(),
                    open_tiers(&config.local_tiers, None, None)?,
                );
            }
            Ok(copal_server::app::Residencies {
                local: open_blobs()?,
                named,
                tiers,
            })
        };
    // Tiers validate at boot, whole-deployment, BEFORE any backend
    // opens: names hold the residency alphabet, archive classes
    // refuse until recall ships, and no tier carries its own key. A
    // deployment cannot come up with a tier its policies could
    // strand bytes behind.
    let tiering = {
        let mut topology = copal_server::tiering::Topology::default();
        let mut register =
            |residency: &str,
             tiers: &std::collections::HashMap<String, copal_blob::tier::TierConfig>|
             -> Result<(), Box<dyn std::error::Error>> {
                copal_blob::tier::validate_tiers(residency, tiers)?;
                let classes = tiers
                    .iter()
                    .map(|(name, tier)| (name.clone(), tier.class))
                    .collect();
                topology.insert(residency, classes);
                Ok(())
            };
        register("local", &config.local_tiers)?;
        for (name, residency_config) in &config.residencies {
            register(name, &residency_config.tiers)?;
        }
        topology
    };
    let residencies = build_residencies()?;
    if !tiering.is_empty() {
        tracing::info!("storage tiers configured; observe-only classifier active");
    }
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
    // Restore drivers ride beside the topology: one per tier that
    // speaks a restore dialect, executed by the recall flow.
    let restore_drivers: copal_server::recall::RestoreDrivers = {
        let mut map = std::collections::HashMap::new();
        let mut add =
            |residency: &str,
             tiers: &std::collections::HashMap<String, copal_blob::tier::TierConfig>| {
                let drivers: std::collections::HashMap<_, _> = tiers
                    .iter()
                    .filter_map(|(name, tier)| {
                        copal_server::recall::RestoreDriver::from_spec(&tier.restore())
                            .map(|driver| (name.clone(), driver))
                    })
                    .collect();
                if !drivers.is_empty() {
                    map.insert(residency.to_owned(), drivers);
                }
            };
        add("local", &config.local_tiers);
        for (name, residency_config) in &config.residencies {
            add(name, &residency_config.tiers);
        }
        map
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
        config.transformers.clone(),
        copal_server::pipeline::FetchPolicy {
            allow_private_targets: config.allow_private_fetch_targets,
            max_bytes: config.max_upload_bytes as u64,
        },
        tiering.clone(),
        restore_drivers,
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
        .with_session_cache(config.session_cache_secs, config.session_cache_size)
        .with_flow(registry.clone())
        .with_auth(config.auth.clone())
        .with_residencies(residencies.named.clone())
        .with_tiering(tiering.clone())
        .with_scan_gate(config.clamav_addr.is_some())
        .with_transformers(config.transformers.clone())
        .with_fleet(config.console_fleet.then(|| config.store.clone()))
        .with_cipher(match &config.blob_encryption_key {
            Some(key) => {
                let cipher = copal_blob::crypto::BlobCipher::from_hex(key)?;
                Some(match &config.blob_encryption_key_previous {
                    Some(previous) => cipher.with_previous(previous)?,
                    None => cipher,
                })
            }
            None => None,
        })
        .with_embedding(
            config
                .embedding_addr
                .clone()
                .map(|addr| (addr, config.embedding_model.clone())),
        )
        .with_reranker(
            config
                .rerank_addr
                .clone()
                .map(|addr| copal_server::rerank::Reranker {
                    addr,
                    model: config.rerank_model.clone(),
                    token: config.rerank_token.clone(),
                    depth: config.rerank_depth,
                }),
        );
    // Tier backends serve reads as well as moves. Once the mover
    // demotes a blob and erases its hot copy, the request path finds
    // the bytes through these, and an archive-cold placement answers
    // with a recall.
    state.residencies.tiers = residencies.tiers.clone();
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
        allow_private_fetch_targets: config.allow_private_fetch_targets,
    };
    if config.allow_private_webhook_targets {
        tracing::warn!(
            "webhook targets on private addresses are ALLOWED              (COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS): tenant-supplied URLs can reach              services inside this deployment",
        );
    }
    match config.engine_sessions.as_str() {
        "off" => {}
        "on" => {
            if config.store.engine_access_key.is_none() {
                return Err(
                    "COPAL_ENGINE_SESSIONS=on requires COPAL_ENGINE_ACCESS_KEY: caller \
                     sessions authenticate against the access method it defines"
                        .into(),
                );
            }
            state.engine_sessions = true;
            tracing::info!("request repository calls run on caller-bound engine sessions");
        }
        other => {
            return Err(format!("COPAL_ENGINE_SESSIONS must be off or on, not {other:?}",).into());
        }
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
            let gateway = gateway.layer(axum::middleware::from_fn(copal_server::trace::middleware));
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

    // A retiring master key drains through two motions: sealed
    // database secrets re-seal once here, before traffic, and sealed
    // objects re-seal through the background sweep until passes come
    // back empty.
    if let Some(cipher) = state.cipher.as_ref().filter(|c| c.has_previous()) {
        copal_server::rotate::reseal_secrets(&store, cipher).await;
    }
    let mut rotating: Vec<(String, ObjectStore)> = Vec::new();
    if residencies.local.is_rotating() {
        rotating.push(("local".to_owned(), residencies.local.clone()));
    }
    for (name, store) in &residencies.named {
        if store.is_rotating() {
            rotating.push((name.clone(), store.clone()));
        }
    }
    // A rotation covers hot and cold copies alike: the reseal sweep
    // walks every tier backend beside its residency.
    for (residency, tiers) in &residencies.tiers {
        for (tier, store) in tiers {
            if store.is_rotating() {
                rotating.push((format!("{residency}/{tier}"), store.clone()));
            }
        }
    }
    if !rotating.is_empty() {
        tracing::info!(
            residencies = rotating.len(),
            "master key rotation under way; object sweep running",
        );
        tokio::spawn(copal_server::rotate::run_sweep(rotating));
    }

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
                let admin = admin.layer(axum::middleware::from_fn(copal_server::trace::middleware));
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
        tiering,
    ));

    // Embedding backfill: a model change drains old geometry in the
    // background, batch by batch, until nothing stale remains.
    if let Some(addr) = config.embedding_addr.clone() {
        let model = config.embedding_model.clone();
        let backfill_store = store.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                ticker.tick().await;
                match copal_server::embed::backfill_pass(&backfill_store, &addr, &model, 64).await {
                    Ok(0) => {}
                    Ok(refreshed) => {
                        tracing::info!(refreshed, model = %model, "embedding backfill pass");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "embedding backfill pass failed");
                    }
                }
            }
        });
    }

    // Durable execution worker: claims pending runs and executes them
    // over the journal. The production registry starts empty until the
    // processing activities land; the worker idles harmlessly.
    tokio::spawn(copal_flow::run_worker(
        copal_flow::FlowEngine::new(store.clone(), registry),
        format!(
            "worker-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ),
        2,
    ));

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    // Drain in-flight requests on SIGTERM or ctrl-c. Background tasks
    // stop with the process; their leases make that safe.
    let router = router.layer(axum::middleware::from_fn(copal_server::trace::middleware));
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

/// Log subscriber, and the OTLP trace pipeline when an endpoint is
/// configured. Without one, spans feed the logs and nothing leaves
/// the process. The exporter speaks http/protobuf, batched on the
/// runtime; traces flush on the batch cadence, so a hard kill can
/// lose the tail of the last batch.
fn init_tracing(otlp_endpoint: Option<&str>) {
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "copal_server=info,copal_store=info".into());
    let fmt = tracing_subscriber::fmt::layer();

    match otlp_endpoint {
        Some(endpoint) => {
            use opentelemetry_otlp::WithExportConfig as _;
            opentelemetry::global::set_text_map_propagator(
                opentelemetry_sdk::propagation::TraceContextPropagator::new(),
            );
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_endpoint(endpoint)
                .build()
                .expect("OTLP exporter builds from COPAL_OTLP_ENDPOINT");
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    opentelemetry_sdk::Resource::builder()
                        .with_service_name("copal")
                        .build(),
                )
                .build();
            use opentelemetry::trace::TracerProvider as _;
            let tracer = provider.tracer("copal");
            opentelemetry::global::set_tracer_provider(provider);
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt)
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
            tracing::info!(endpoint = %endpoint, "OTLP trace export enabled");
        }
        None => {
            tracing_subscriber::registry().with(filter).with(fmt).init();
        }
    }
}
