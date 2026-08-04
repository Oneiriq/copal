//! copalctl: the Copal service from a terminal.
//!
//! Output is JSON, always: every command prints exactly what the
//! API answered (or a small object the command assembled), so the
//! terminal composes with jq and scripts the way the API composes
//! with programs. Identity comes from the environment: `COPAL_URL`,
//! then `COPAL_TOKEN` (a minted key) or `COPAL_TENANT` (dev-mode
//! header identity), and `COPAL_ADMIN_TOKEN` for admin commands.

use clap::{Parser, Subcommand};
use copal_cli::Api;
use serde_json::{json, Value};

#[derive(Parser)]
#[command(
    name = "copalctl",
    version,
    about = "The Copal service from a terminal."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Service health: healthz and readyz in one answer.
    Status,
    /// Files: the primary plane.
    Files {
        #[command(subcommand)]
        command: Files,
    },
    /// Retrieval across the tenant's extracted text.
    Search {
        /// The question.
        q: String,
        #[arg(long)]
        limit: Option<u32>,
        /// Narrow to paths under this prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// Narrow to one declared content type.
        #[arg(long)]
        content_type: Option<String>,
    },
    /// The admin surface; needs COPAL_ADMIN_TOKEN.
    Admin {
        #[command(subcommand)]
        command: Admin,
    },
}

#[derive(Subcommand)]
enum Files {
    /// One page of the tenant's files.
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        cursor: Option<String>,
        /// Narrow to one state (ready, quarantined, ...).
        #[arg(long)]
        state: Option<String>,
    },
    /// One file's record.
    Get { id: String },
    /// Create a record; upload bytes with `files upload`.
    Create {
        path: String,
        #[arg(long)]
        content_type: Option<String>,
        /// public, private, tenant, or grant.
        #[arg(long)]
        access: Option<String>,
    },
    /// Upload a local file's bytes to a record.
    Upload {
        id: String,
        /// Local path to read.
        local: std::path::PathBuf,
    },
    /// Download a record's bytes.
    Download {
        id: String,
        /// Local path to write; stdout when absent.
        #[arg(short, long)]
        out: Option<std::path::PathBuf>,
    },
    /// Remove a file.
    Remove { id: String },
}

#[derive(Subcommand)]
enum Admin {
    /// Every tenant that has stored anything, with files and bytes.
    Tenants,
    /// The deployment audit trail, newest page by default.
    Audit {
        #[arg(long)]
        limit: Option<u32>,
        /// Resume from a saved cursor (ascending replay).
        #[arg(long)]
        cursor: Option<String>,
        /// Narrow to one tenant.
        #[arg(long)]
        tenant: Option<String>,
    },
    /// API keys for one tenant.
    Keys {
        tenant: String,
        #[command(subcommand)]
        command: Keys,
    },
}

#[derive(Subcommand)]
enum Keys {
    /// Mint a key; the token prints once.
    Mint {
        #[arg(long)]
        name: String,
    },
    /// List the tenant's keys.
    List,
    /// Revoke a key.
    Revoke { key_id: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let api = Api::from_env();
    let answer = run(&api, cli.command).await?;
    if !answer.is_null() {
        println!("{}", serde_json::to_string_pretty(&answer)?);
    }
    Ok(())
}

async fn run(api: &Api, command: Command) -> anyhow::Result<Value> {
    match command {
        Command::Status => {
            let healthz = api.get("/healthz", &[]).await;
            let readyz = api.get("/readyz", &[]).await;
            Ok(json!({
                "base": api.base,
                "healthz": healthz.map(|_| "ok").unwrap_or("refused"),
                "readyz": readyz.map(|_| "ok").unwrap_or("refused"),
            }))
        }
        Command::Files { command } => files(api, command).await,
        Command::Search {
            q,
            limit,
            prefix,
            content_type,
        } => {
            let mut query = vec![("q", q)];
            if let Some(limit) = limit {
                query.push(("limit", limit.to_string()));
            }
            if let Some(prefix) = prefix {
                query.push(("prefix", prefix));
            }
            if let Some(content_type) = content_type {
                query.push(("content_type", content_type));
            }
            api.get("/v1/search", &query).await
        }
        Command::Admin { command } => admin(api, command).await,
    }
}

async fn files(api: &Api, command: Files) -> anyhow::Result<Value> {
    match command {
        Files::List {
            limit,
            cursor,
            state,
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit", limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor", cursor));
            }
            if let Some(state) = state {
                query.push(("state", state));
            }
            api.get("/v1/files", &query).await
        }
        Files::Get { id } => api.get(&format!("/v1/files/{id}"), &[]).await,
        Files::Create {
            path,
            content_type,
            access,
        } => {
            let mut body = serde_json::Map::new();
            body.insert("path".into(), json!(path));
            if let Some(content_type) = content_type {
                body.insert("content_type".into(), json!(content_type));
            }
            if let Some(access) = access {
                body.insert("access".into(), json!(access));
            }
            api.post("/v1/files", Value::Object(body)).await
        }
        Files::Upload { id, local } => {
            let bytes = std::fs::read(&local)?;
            api.put_bytes(&format!("/v1/files/{id}/content"), bytes)
                .await
        }
        Files::Download { id, out } => {
            let bytes = api.get_bytes(&format!("/v1/files/{id}/content")).await?;
            match out {
                Some(path) => {
                    std::fs::write(&path, &bytes)?;
                    Ok(json!({ "wrote": path, "bytes": bytes.len() }))
                }
                None => {
                    use std::io::Write as _;
                    std::io::stdout().write_all(&bytes)?;
                    Ok(Value::Null)
                }
            }
        }
        Files::Remove { id } => api.delete(&format!("/v1/files/{id}")).await,
    }
}

async fn admin(api: &Api, command: Admin) -> anyhow::Result<Value> {
    match command {
        Admin::Tenants => api.admin_get("/v1/admin/tenants", &[]).await,
        Admin::Audit {
            limit,
            cursor,
            tenant,
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit", limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor", cursor));
            }
            if let Some(tenant) = tenant {
                query.push(("tenant", tenant));
            }
            let (body, next) = api.admin_text("/v1/admin/audit/export", &query).await?;
            let events: Vec<Value> = body
                .lines()
                .filter(|line| !line.is_empty())
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect();
            Ok(json!({ "events": events, "next_cursor": next }))
        }
        Admin::Keys { tenant, command } => match command {
            Keys::Mint { name } => {
                api.admin_post(
                    &format!("/v1/admin/tenants/{tenant}/keys"),
                    json!({ "name": name }),
                )
                .await
            }
            Keys::List => {
                api.admin_get(&format!("/v1/admin/tenants/{tenant}/keys"), &[])
                    .await
            }
            Keys::Revoke { key_id } => {
                api.admin_delete(&format!("/v1/admin/tenants/{tenant}/keys/{key_id}"))
                    .await
            }
        },
    }
}
