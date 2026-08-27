//! The operator console on the admin surface: Basic auth against
//! the admin token, a deployment home only copal can render, and
//! the kayak contract pages per tenant, dispatched through the same
//! chain every API face uses.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::json;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    });
    (build_router(state), dir)
}

fn basic() -> String {
    let pair = base64::engine::general_purpose::STANDARD.encode(format!("operator:{ADMIN}"));
    format!("Basic {pair}")
}

async fn text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Mint a key, create a file, upload bytes: one tenant with content,
/// through the API the console will mirror.
async fn seed(router: &axum::Router) -> String {
    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/keys")
        .header("x-copal-admin-token", ADMIN)
        .header("content-type", "application/json")
        .body(Body::from(json!({ "name": "console-test" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(mint).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let token = serde_json::from_str::<serde_json::Value>(&text(response).await).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();

    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "path": "ops/manual.txt" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = serde_json::from_str::<serde_json::Value>(&text(response).await).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(&b"console proof"[..]))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn the_console_gates_on_the_admin_token() {
    let (router, _dir) = stack().await;

    let bare = Request::builder()
        .uri("/admin/console")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(bare).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        response.headers().contains_key("www-authenticate"),
        "the browser gets a Basic challenge",
    );

    let wrong = Request::builder()
        .uri("/admin/console")
        .header(
            "authorization",
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("operator:guess"),
            ),
        )
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(wrong).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_deployment_home_names_tenants_and_tails_the_audit() {
    let (router, _dir) = stack().await;
    seed(&router).await;

    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(html.contains("/admin/console/t/acme"), "the tenant links");
    assert!(html.contains("key.minted"), "the audit tail shows custody");
}

#[tokio::test]
async fn the_tenant_console_serves_the_contract_pages() {
    let (router, _dir) = stack().await;
    let id = seed(&router).await;

    let overview = Request::builder()
        .uri("/admin/console/t/acme")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(overview).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(text(response)
        .await
        .contains("/admin/console/t/acme/r/files"));

    let listing = Request::builder()
        .uri("/admin/console/t/acme/r/files")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(listing).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(html.contains(&id), "the uploaded file lists");
    assert!(html.contains("ops/manual.txt"));

    let detail = Request::builder()
        .uri(format!("/admin/console/t/acme/r/files/{id}"))
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(detail).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    // Headings say the name the way a person writes it, so a
    // sub-collection declared as `versions` reads as "Versions".
    assert!(html.contains("Versions"), "sub-collections render");
    assert!(html.contains("/a/issue_url"), "actions become forms");
}

/// An action whose answer is the point shows that answer. A grant is
/// unrecoverable once the page is gone, so the console must not throw
/// it away on a redirect.
#[tokio::test]
async fn a_console_form_shows_what_the_action_answered() {
    let (router, _dir) = stack().await;
    let id = seed(&router).await;

    let submit = Request::builder()
        .method("POST")
        .uri(format!("/admin/console/t/acme/r/files/{id}/a/issue_url"))
        .header("authorization", basic())
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("ttl_secs=60"))
        .unwrap();
    let response = router.clone().oneshot(submit).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(
        html.contains("/v1/grants/cg1."),
        "the issued URL is on the page the operator lands on",
    );
    assert!(
        html.contains(&format!("/admin/console/t/acme/r/files/{id}")),
        "with a way back to the instance",
    );
}

#[tokio::test]
async fn the_fleet_view_gates_on_configuration_and_names_its_limits() {
    // Off by default: no fleet section at all.
    let (router, _dir) = stack().await;
    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    let html = text(response).await;
    assert!(
        !html.contains(">fleet<"),
        "no fleet section unless configured"
    );

    // Configured against an embedded engine, the walk refuses with
    // its reason instead of pretending.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
            operator_header: None,
        })
        .with_fleet(Some(StoreConfig::memory()));
    let router = build_router(state);
    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    let html = text(response).await;
    assert!(
        html.contains(">Fleet<"),
        "the section renders when configured"
    );
    assert!(
        html.contains("needs a remote engine"),
        "an unwalkable engine is named, never faked",
    );
}

/// The link the deployment home actually emits must reach the tenant.
/// The earlier tests requested a URL they built themselves, which is
/// how a dead link survived them.
#[tokio::test]
async fn every_link_the_home_emits_resolves() {
    let (router, _dir) = stack().await;
    seed(&router).await;

    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let html = text(router.clone().oneshot(home).await.unwrap()).await;

    let links: Vec<String> = html
        .split("href=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .filter(|href| href.starts_with('/'))
        .map(str::to_owned)
        .collect();
    assert!(
        links.iter().any(|href| href.contains("/t/acme")),
        "the home links its tenants",
    );

    for href in links {
        let request = Request::builder()
            .uri(&href)
            .header("authorization", basic())
            .body(Body::empty())
            .unwrap();
        let status = router.clone().oneshot(request).await.unwrap().status();
        assert!(
            status.is_success(),
            "the home emits {href}, which answers {status}",
        );
    }
}

/// A bare base with a trailing slash is what a browser makes of a
/// link to the tenant overview, so it answers.
#[tokio::test]
async fn the_tenant_overview_answers_under_both_spellings() {
    let (router, _dir) = stack().await;
    seed(&router).await;
    for uri in ["/admin/console/t/acme", "/admin/console/t/acme/"] {
        let request = Request::builder()
            .uri(uri)
            .header("authorization", basic())
            .body(Body::empty())
            .unwrap();
        let status = router.clone().oneshot(request).await.unwrap().status();
        assert_eq!(status, StatusCode::OK, "{uri} must answer");
    }
}

/// Every request the reference offers is one that would actually run.
///
/// The page exists so a caller can copy a request out of it. An
/// example that does not parse is worse than no example, and it fails
/// silently: the page renders, the text looks right, and nobody finds
/// out until someone pastes it. This has caught a `mutation` with no
/// selection set on an action that returns an object, a selection set
/// on one that returns a scalar, and a REST body carrying comments
/// that are not JSON.
///
/// The check used to live in a Node script beside the repo, which
/// meant it ran when somebody remembered to run it.
#[tokio::test]
async fn every_example_on_the_reference_would_run() {
    let (router, _dir) = stack().await;
    let token = seed(&router).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/console/t/acme/reference")
                .header("authorization", basic())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = text(response).await;

    let mut documents = 0;
    let mut bodies = 0;
    for block in code_blocks(&page, "GraphQL") {
        documents += 1;
        async_graphql::parser::parse_query(&block)
            .unwrap_or_else(|e| panic!("a GraphQL example does not parse: {e}\n{block}"));

        // Parsing is not enough: a field that does not exist, or a
        // selection set on a scalar, parses and then refuses on the
        // way in. So each one is sent to the endpoint this deployment
        // actually serves. A resolver saying it found nothing is the
        // example working; the schema saying it will not take the
        // document is the example being wrong.
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "query": block }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let said = text(response).await;
        for refusal in [
            "Unknown field",
            "Cannot query field",
            "must have a selection set",
            "Unknown argument",
            "is not defined",
            "Syntax Error",
            "expected type",
            "Unknown type",
            "Missing argument",
        ] {
            assert!(
                !said.contains(refusal),
                "the schema will not take an example ({refusal}):\n{block}\n{said}",
            );
        }
    }
    assert!(documents >= 10, "the page offers examples: {documents}");

    for block in code_blocks(&page, "REST") {
        // The request line, then a body if there is one.
        let Some((_, body)) = block.split_once("\n\n") else {
            continue;
        };
        if body.trim().is_empty() {
            continue;
        }
        bodies += 1;
        serde_json::from_str::<serde_json::Value>(body.trim())
            .unwrap_or_else(|e| panic!("a REST body is not JSON: {e}\n{body}"));
    }
    assert!(bodies > 0, "and at least one carries a body");
}

/// The schema section rebuilds into a schema.
///
/// The section is cut out of the generated SDL by matching braces. A
/// splitter that drops a block or runs two together still renders
/// something that looks like a schema.
#[tokio::test]
async fn the_schema_the_reference_prints_is_a_schema() {
    let (router, _dir) = stack().await;
    seed(&router).await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/console/t/acme/reference")
                .header("authorization", basic())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let page = text(response).await;

    let mut pieces = Vec::new();
    let mut rest = page.as_str();
    while let Some(at) = rest.find("<section class=\"definition\"") {
        rest = &rest[at..];
        let Some(end) = rest.find("</section>") else {
            break;
        };
        let section = &rest[..end];
        rest = &rest[end..];
        match section.find("<pre><code>") {
            Some(open) => {
                let body = &section[open + "<pre><code>".len()..];
                let close = body.find("</code>").unwrap_or(body.len());
                pieces.push(unescape(strip_tags(&body[..close])));
            }
            // A scalar is its own head line and prints no body.
            None => {
                if let Some(kind) = between(section, "<span class=\"chip\">", "</span>") {
                    if let Some(name) =
                        between(section, "<code class=\"definition-name\">", "</code>")
                    {
                        pieces.push(format!("{kind} {name}"));
                    }
                }
            }
        }
    }
    assert!(
        pieces.len() >= 10,
        "the page prints a schema: {}",
        pieces.len()
    );
    let sdl = pieces.join("\n\n");
    async_graphql::parser::parse_schema(&sdl)
        .unwrap_or_else(|e| panic!("the printed schema does not parse: {e}\n{sdl}"));
}

/// The text of every `<pre><code>` under a heading, unescaped.
fn code_blocks(page: &str, heading: &str) -> Vec<String> {
    let opener = format!("{heading}</div><pre><code>");
    let mut out = Vec::new();
    let mut rest = page;
    while let Some(at) = rest.find(&opener) {
        let body = &rest[at + opener.len()..];
        let end = body.find("</code>").unwrap_or(body.len());
        out.push(unescape(strip_tags(&body[..end])));
        rest = &body[end..];
    }
    out
}

fn between<'a>(haystack: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = haystack.find(open)? + open.len();
    let end = haystack[start..].find(close)? + start;
    Some(&haystack[start..end])
}

/// Type names inside the printed SDL are links, so the tags come out
/// before the text is read as a schema.
fn strip_tags(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut inside = false;
    for ch in raw.chars() {
        match ch {
            '<' => inside = true,
            '>' => inside = false,
            other if !inside => out.push(other),
            _ => {}
        }
    }
    out
}

fn unescape(raw: String) -> String {
    raw.replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// The refusal words the check above watches for are the words the
/// schema actually uses.
///
/// That check is only as good as its list: if async-graphql changes
/// how it phrases a refusal, the examples would stop being checked
/// and every one of them would pass. So four documents that are wrong
/// in four different ways are sent through, and each has to be
/// caught by the same list.
#[tokio::test]
async fn the_refusal_words_are_the_right_words() {
    let (router, _dir) = stack().await;
    let token = seed(&router).await;
    for bad in [
        "{ nosuchfield { id } }",
        "{ files { items { nosuchsubfield } } }",
        "{ files(nosucharg: 1) { items { id } } }",
        "mutation { fileIssueUrl(id: \"x\", ttlSecs: 1) { nope } }",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "query": bad }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let said = text(response).await;
        let caught = [
            "Unknown field",
            "Cannot query field",
            "must have a selection set",
            "Unknown argument",
            "is not defined",
            "Syntax Error",
            "expected type",
            "Unknown type",
            "Missing argument",
        ]
        .iter()
        .any(|w| said.contains(w));
        assert!(
            caught,
            "a broken document was not caught: {bad}
{said}"
        );
    }
}
