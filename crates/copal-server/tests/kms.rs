//! The key-custody seam against a live service.
//!
//! The master key decides whether stored bytes can be read at all, so
//! the boot path either gets a real key from custody or refuses. What
//! it must never do is come up holding something that is not a key.

use copal_server::kms;

/// A custody service that answers whatever the test hands it.
async fn custody(answer: &'static str, status: u16) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().route(
        "/keys/{key_id}",
        axum::routing::get(move |headers: axum::http::HeaderMap| async move {
            let presented = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            if presented != "Bearer right-token" {
                return (
                    axum::http::StatusCode::FORBIDDEN,
                    [("content-type", "application/json")],
                    r#"{"error":"bearer token does not match"}"#.to_owned(),
                );
            }
            (
                axum::http::StatusCode::from_u16(status).unwrap(),
                [("content-type", "application/json")],
                answer.to_owned(),
            )
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test]
async fn custody_hands_over_the_key() {
    let current = "a".repeat(64);
    let addr = custody(
        r#"{"current":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        200,
    )
    .await;
    let material = kms::fetch(&addr, "blob", Some("right-token"))
        .await
        .unwrap();
    assert_eq!(material.current, current);
    assert!(material.previous.is_none(), "no rotation is under way");
}

/// A rotation is a state, so both halves arrive together and an
/// operator never sees a half that never existed.
#[tokio::test]
async fn custody_carries_a_rotation() {
    let addr = custody(
        r#"{"current":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "previous":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        200,
    )
    .await;
    let material = kms::fetch(&addr, "blob", Some("right-token"))
        .await
        .unwrap();
    assert_eq!(material.current, "b".repeat(64));
    assert_eq!(material.previous.as_deref(), Some("a".repeat(64).as_str()));
}

/// Every refusal is an error the boot path reports, because a
/// deployment configured for custody must not quietly read the
/// environment instead.
#[tokio::test]
async fn custody_that_refuses_stops_the_boot() {
    let addr = custody(r#"{"current":"irrelevant"}"#, 200).await;
    let wrong_token = kms::fetch(&addr, "blob", Some("guess")).await;
    assert!(wrong_token.is_err(), "a bad token is a refusal");
    assert!(
        wrong_token.unwrap_err().to_string().contains("403"),
        "the refusal carries what custody said",
    );

    let unreachable = kms::fetch("http://127.0.0.1:9", "blob", None).await;
    assert!(unreachable.is_err(), "an unreachable custody is a refusal");
}

/// Something that is not a key must be caught here, where the cause
/// is obvious, rather than at the first read of a sealed object.
#[tokio::test]
async fn an_answer_that_is_not_a_key_is_refused() {
    let short = custody(r#"{"current":"abc123"}"#, 200).await;
    let error = kms::fetch(&short, "blob", Some("right-token"))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("64 hex"),
        "the error names the shape: {error}"
    );

    let not_hex = custody(
        r#"{"current":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}"#,
        200,
    )
    .await;
    assert!(kms::fetch(&not_hex, "blob", Some("right-token"))
        .await
        .is_err());

    // The retiring key is held to the same shape.
    let bad_previous = custody(
        r#"{"current":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "previous":"nonsense"}"#,
        200,
    )
    .await;
    let error = kms::fetch(&bad_previous, "blob", Some("right-token"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("previous"), "it says which key: {error}");
}
