use super::*;
use super::{callbacks::*, storage::*, webhooks::*};
use axum::http::Method;
use axum::{
    body::Body,
    http::{Request, StatusCode as HttpStatusCode},
};
use tower::ServiceExt;

static TEST_DATABASE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_state() -> AppState {
    let db = PgPool::connect_lazy("postgres://postgres@localhost/rust-webhooks")
        .expect("test db URL should parse");

    AppState {
        db,
        sensitive_headers: SensitiveKeysConfig {
            exact_matches: vec![],
            contains: vec![],
            suffix: vec![],
        },
        sensitive_query_keys: SensitiveKeysConfig {
            exact_matches: vec![],
            contains: vec![],
            suffix: vec![],
        },
        default_ttl_seconds: 300,
        callback_pending_ttl_seconds: 14 * 24 * 60 * 60,
        callback_retention_seconds: 72 * 60 * 60,
        callback_max_body_bytes: 128,
        callback_rate_limit_per_minute: 120,
        callback_rate_limits: Arc::default(),
        subscriptions: Arc::default(),
        webhooks_changed: watch::channel(()).0,
    }
}

fn test_app() -> Router {
    build_app(test_state())
}

#[tokio::test]
async fn callback_rate_limit_is_per_ip_and_resets_after_one_minute() {
    let mut state = test_state();
    state.callback_rate_limit_per_minute = 2;
    let now = Instant::now();
    let ip: IpAddr = "192.0.2.1".parse().unwrap();
    let other_ip: IpAddr = "192.0.2.2".parse().unwrap();

    assert!(allow_callback_request(&state, ip, now));
    assert!(allow_callback_request(&state, ip, now));
    assert!(!allow_callback_request(&state, ip, now));
    assert!(allow_callback_request(&state, other_ip, now));
    assert!(allow_callback_request(
        &state,
        ip,
        now + Duration::from_secs(60)
    ));
}

#[test]
fn callback_webhook_id_accepts_only_single_uuid_segments() {
    let id = Uuid::new_v4();
    assert_eq!(
        callback_webhook_id(&format!("/api/webhooks/{id}")),
        Some(id)
    );
    assert_eq!(callback_webhook_id("/api/webhooks/not-a-uuid"), None);
    assert_eq!(
        callback_webhook_id(&format!("/api/webhooks/{id}/nested")),
        None
    );
    assert_eq!(callback_webhook_id("/api/webhooks"), None);
}

#[tokio::test]
async fn callback_creation_is_rate_limited_before_database_access() {
    let mut state = test_state();
    state.callback_rate_limit_per_minute = 1;
    let app = build_app(state);
    let request = || {
        Request::builder()
            .method(Method::POST)
            .uri("/api/callbacks")
            .body(Body::empty())
            .expect("request should build")
    };

    let first = app
        .clone()
        .oneshot(request())
        .await
        .expect("response should be returned");
    assert_eq!(first.status(), HttpStatusCode::INTERNAL_SERVER_ERROR);

    let second = app
        .oneshot(request())
        .await
        .expect("response should be returned");
    assert_eq!(second.status(), HttpStatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn callback_delivery_rejects_invalid_and_oversized_bodies_without_database_access() {
    let mut state = test_state();
    state.callback_max_body_bytes = 8;
    let app = build_app(state);
    let id = Uuid::new_v4();

    for (body, expected_status) in [
        ("not-json", HttpStatusCode::BAD_REQUEST),
        (
            r#"{"payload":"too large"}"#,
            HttpStatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/webhooks/{id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("response should be returned");
        assert_eq!(response.status(), expected_status);
    }
}

fn topic(tenant: &str, app: &str, event: Option<&str>) -> WebhookTopic {
    WebhookTopic {
        tenant: tenant.to_string(),
        app: app.to_string(),
        event: event.map(str::to_string),
    }
}

#[test]
fn websocket_topic_overlap_preserves_http_event_wildcard_semantics() {
    let all_events = topic("tenant", "app", None);
    let exact = topic("tenant", "app", Some("event"));
    assert!(all_events.overlaps(&exact));
    assert!(exact.overlaps(&all_events));
    assert!(exact.overlaps(&exact));
    assert!(all_events.overlaps(&all_events));
    assert!(!exact.overlaps(&topic("other", "app", None)));
    assert!(!exact.overlaps(&topic("tenant", "other", None)));
    assert!(!exact.overlaps(&topic("tenant", "app", Some("other"))));
}

#[test]
fn wildcard_topics_overlap_only_matching_subsets() {
    let exact = topic("tenant", "app", Some("event"));
    for wildcard in [
        topic("tenant", "app", Some("*")),
        topic("tenant", "*", Some("*")),
        topic("tenant", "*", None),
        topic("*", "*", Some("*")),
        topic("*", "*", None),
    ] {
        assert!(wildcard.overlaps(&exact));
        assert!(exact.overlaps(&wildcard));
        assert!(wildcard.overlaps(&wildcard));
    }
    assert!(!topic("tenant", "*", Some("*")).overlaps(&topic("other", "app", None)));
    assert!(!topic("tenant", "app", Some("*")).overlaps(&topic("tenant", "other", None)));
}

#[tokio::test]
async fn consumer_routes_reject_non_trailing_wildcards() {
    let state = test_state();
    let app = build_app(state.clone());
    let (base, server) = start_test_server(state.clone()).await;
    for scope in [
        "*/app",
        "*/app/event",
        "*/*/event",
        "*/app/*",
        "tenant/*/event",
        "tenant/%2A/event",
    ] {
        for (method, action) in [(Method::GET, "peek"), (Method::POST, "receive")] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(format!("/api/consumer/{action}/{scope}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{action}/{scope}"
            );
        }
        let error = tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/{scope}"))
            .await
            .unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{scope}");
            }
            other => panic!("expected invalid wildcard, got {other}"),
        }
    }
    assert!(state.subscriptions.topics.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn websocket_registration_is_atomic_and_released_on_drop() {
    let registry = Arc::new(WebsocketSubscriptions::default());
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let registry = registry.clone();
        tasks.push(tokio::spawn(async move {
            registry.register(topic("tenant", "app", None)).await.ok()
        }));
    }
    let mut subscriptions = Vec::new();
    for task in tasks {
        if let Some(subscription) = task.await.unwrap() {
            subscriptions.push(subscription);
        }
    }
    assert_eq!(subscriptions.len(), 1);
    assert!(registry.conflicts(&topic("tenant", "app", Some("event"))));
    drop(subscriptions);
    assert!(!registry.conflicts(&topic("tenant", "app", None)));
    assert!(registry
        .register(topic("tenant", "app", None))
        .await
        .is_ok());
}

#[tokio::test]
async fn websocket_registration_waits_for_in_flight_http_receive() {
    let registry = Arc::new(WebsocketSubscriptions::default());
    let claim_guard = registry
        .reserve(topic("tenant", "app", Some("event")), false)
        .unwrap();
    let subscriber_registry = registry.clone();
    let mut registration = tokio::spawn(async move {
        subscriber_registry
            .register(topic("tenant", "app", None))
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut registration)
            .await
            .is_err()
    );
    assert!(!registry.conflicts(&topic("tenant", "app", None)));
    let unrelated = tokio::time::timeout(
        Duration::from_millis(100),
        registry.register(topic("tenant", "app", Some("other"))),
    )
    .await
    .unwrap()
    .unwrap();
    let other_tenant = registry
        .register(topic("other", "app", None))
        .await
        .unwrap();
    let other_http = registry
        .reserve(topic("tenant", "app", Some("event")), false)
        .unwrap();
    drop(unrelated);
    drop(other_tenant);
    drop(other_http);
    drop(claim_guard);
    let subscription = registration.await.unwrap();
    assert!(registry.conflicts(&topic("tenant", "app", None)));
    drop(subscription);
}

async fn start_test_server(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, build_app(state)).await.unwrap();
    });
    (format!("ws://{address}"), server)
}

#[tokio::test]
async fn websocket_conflicts_block_only_overlapping_receive_and_release_on_disconnect() {
    let state = test_state();
    let mut http_state = state.clone();
    http_state.db = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://postgres@localhost/rust-webhooks")
        .unwrap();
    let app = build_app(http_state);
    let (base, server) = start_test_server(state.clone()).await;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/tenant/app/event"))
            .await
            .unwrap();

    for scope in [
        "tenant/app/event",
        "tenant/app",
        "tenant/app/*",
        "tenant/*/*",
        "*/*/*",
        "tenant/*",
        "*/*",
    ] {
        let error = tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/{scope}"))
            .await
            .unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::CONFLICT);
            }
            other => panic!("expected HTTP conflict, got {other}"),
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/consumer/receive/{scope}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    for path in [
        "/api/consumer/peek/tenant/app",
        "/api/consumer/peek/tenant/app/event",
        "/api/consumer/receive/tenant/app/other",
        "/api/consumer/receive/other/app/event",
        "/api/consumer/receive/tenant/other/event",
        "/api/operator/status",
    ] {
        let method = if path.contains("/receive/") {
            Method::POST
        } else {
            Method::GET
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::CONFLICT, "{path}");
    }

    let (other, _) =
        tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/tenant/app/other"))
            .await
            .unwrap();
    drop(other);
    socket.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !state.subscriptions.topics.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (mut replacement, _) =
        tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/tenant/*/*"))
            .await
            .unwrap();
    replacement.close(None).await.unwrap();
    server.abort();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL database in TEST_DATABASE_URL"]
async fn wildcard_consumers_match_correct_subsets() {
    use axum::body::to_bytes;
    use futures_util::StreamExt;

    let _database_guard = TEST_DATABASE_LOCK.lock().await;
    let database_url = env::var("TEST_DATABASE_URL").unwrap();
    let db = PgPool::connect(&database_url).await.unwrap();
    let schema = format!("wildcard_test_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&db)
        .await
        .unwrap();
    let mut state = test_state();
    state.db = PgPoolOptions::new()
        .connect_with(
            database_url
                .parse::<sqlx::postgres::PgConnectOptions>()
                .unwrap()
                .options([("search_path", schema.as_str())]),
        )
        .await
        .unwrap();
    ensure_schema(&state.db).await.unwrap();
    let app = build_app(state.clone());
    let (base, server) = start_test_server(state.clone()).await;
    let paths = [
        "tenant/app/first",
        "tenant/app/second",
        "tenant/other/third",
        "other/app/fourth",
    ];
    for path in paths {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/webhooks/{path}"))
                    .body(Body::from("payload"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    for (scope, count) in [
        ("tenant/app/first", 1),
        ("tenant/app", 2),
        ("tenant/app/*", 2),
        ("tenant/app/%2A", 2),
        ("tenant/*", 3),
        ("tenant/*/*", 3),
        ("*/*", 4),
        ("*/*/*", 4),
    ] {
        for transport in ["http", "websocket"] {
            sqlx::query("UPDATE incoming_webhooks SET status = 'new', ttl_expires_at = NULL")
                .execute(&state.db)
                .await
                .unwrap();
            let peek = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/consumer/peek/{scope}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(peek.status(), StatusCode::OK, "{scope}");
            let peek: Value =
                serde_json::from_slice(&to_bytes(peek.into_body(), usize::MAX).await.unwrap())
                    .unwrap();
            assert_eq!(peek["path"], format!("/api/webhooks/{}", paths[0]));
            assert_eq!(peek["status"], "new");
            let mut socket = if transport == "websocket" {
                Some(
                    tokio_tungstenite::connect_async(format!(
                        "{base}/api/consumer/ws/{scope}?ttl_seconds=60"
                    ))
                    .await
                    .unwrap()
                    .0,
                )
            } else {
                None
            };
            for path in &paths[..count] {
                let received: Value = if let Some(socket) = &mut socket {
                    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    serde_json::from_str(frame.to_text().unwrap()).unwrap()
                } else {
                    let response = app
                        .clone()
                        .oneshot(
                            Request::builder()
                                .method(Method::POST)
                                .uri(format!("/api/consumer/receive/{scope}?ttl_seconds=60"))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.status(), StatusCode::OK, "{scope}");
                    serde_json::from_slice(
                        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                    )
                    .unwrap()
                };
                assert_eq!(received["path"], format!("/api/webhooks/{path}"), "{scope}");
                assert_eq!(received["status"], "received");
                assert_eq!(received["active"], true);
                let expiry =
                    DateTime::parse_from_rfc3339(received["ttl_expires_at"].as_str().unwrap())
                        .unwrap();
                assert!((expiry.with_timezone(&Utc) - Utc::now()).num_seconds() > 55);
            }
            if let Some(mut socket) = socket {
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), socket.next())
                        .await
                        .is_err(),
                    "{scope}"
                );
                socket.close(None).await.unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !state.subscriptions.topics.lock().unwrap().is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            for action in ["peek", "receive"] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(if action == "peek" {
                                Method::GET
                            } else {
                                Method::POST
                            })
                            .uri(format!("/api/consumer/{action}/{scope}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::NO_CONTENT,
                    "{action}/{scope}"
                );
            }
        }
    }
    server.abort();
    state.db.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&db)
        .await
        .unwrap();
}

#[tokio::test]
async fn websocket_wildcard_blocks_exact_receive_and_invalid_ttl_does_not_reserve_topic() {
    let state = test_state();
    let (base, server) = start_test_server(state.clone()).await;
    let error = tokio_tungstenite::connect_async(format!(
        "{base}/api/consumer/ws/tenant/app?ttl_seconds=0"
    ))
    .await
    .unwrap_err();
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        other => panic!("expected invalid TTL, got {other}"),
    }
    assert!(state.subscriptions.topics.lock().unwrap().is_empty());
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/tenant/app"))
            .await
            .unwrap();
    for scope in ["tenant/app", "tenant/app/first", "tenant/app/second"] {
        let response = build_app(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/consumer/receive/{scope}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let error = tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/{scope}"))
            .await
            .unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::CONFLICT);
            }
            other => panic!("expected HTTP conflict, got {other}"),
        }
    }
    socket.close(None).await.unwrap();
    server.abort();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL database in TEST_DATABASE_URL"]
async fn websocket_delivers_backlog_and_new_webhooks_with_http_receive_lifecycle() {
    use futures_util::StreamExt;

    let _database_guard = TEST_DATABASE_LOCK.lock().await;
    let mut state = test_state();
    state.db = PgPool::connect(&env::var("TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    ensure_schema(&state.db).await.unwrap();
    let app = build_app(state.clone());
    let (base, server) = start_test_server(state.clone()).await;

    for event_scope in [None, Some("first")] {
        let tenant = Uuid::new_v4().to_string();
        let capture = |event: &str| {
            app.clone().oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/webhooks/{tenant}/app/{event}"))
                    .body(Body::from("payload"))
                    .unwrap(),
            )
        };
        assert_eq!(
            capture("first").await.unwrap().status(),
            StatusCode::ACCEPTED
        );
        let scope = match event_scope {
            Some(event) => format!("{tenant}/app/{event}"),
            None => format!("{tenant}/app"),
        };
        let (mut socket, _) = tokio_tungstenite::connect_async(format!(
            "{base}/api/consumer/ws/{scope}?ttl_seconds=45"
        ))
        .await
        .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let backlog: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(backlog["event"], "first");
        assert_eq!(backlog["status"], "received");
        assert_eq!(backlog["body_text"], "payload");
        let expires_at =
            DateTime::parse_from_rfc3339(backlog["ttl_expires_at"].as_str().unwrap()).unwrap();
        assert!((expires_at.with_timezone(&Utc) - Utc::now()).num_seconds() > 40);

        assert_eq!(
            capture("second").await.unwrap().status(),
            StatusCode::ACCEPTED
        );
        if event_scope.is_some() {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), socket.next())
                    .await
                    .is_err()
            );
            let peek = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/consumer/peek/{tenant}/app/second"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(peek.status(), StatusCode::OK);
            assert_eq!(
                capture("first").await.unwrap().status(),
                StatusCode::ACCEPTED
            );
        }
        let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let delivered: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(delivered["event"], event_scope.unwrap_or("second"));
        assert_eq!(delivered["status"], "received");
        assert_ne!(delivered["id"], backlog["id"]);

        let id = backlog["id"].as_str().unwrap();
        let check_in = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!(
                        "/api/consumer/webhooks/{id}/check-in?ttl_seconds=60"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(check_in.status(), StatusCode::OK);
        let complete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/consumer/webhooks/{id}/complete"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"outcome":"success"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::OK);

        socket.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.subscriptions.conflicts(&topic(&tenant, "app", None)) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/consumer/receive/{scope}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        sqlx::query("DELETE FROM incoming_webhooks WHERE tenant = $1")
            .bind(&tenant)
            .execute(&state.db)
            .await
            .unwrap();
    }
    server.abort();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL database in TEST_DATABASE_URL"]
async fn callback_endpoints_enforce_single_fulfillment_and_expiration() {
    use axum::body::to_bytes;

    let _database_guard = TEST_DATABASE_LOCK.lock().await;
    let mut state = test_state();
    state.db = PgPool::connect(&env::var("TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    ensure_schema(&state.db).await.unwrap();
    let app = build_app(state.clone());

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/callbacks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["callback_url"], format!("/api/webhooks/{id}"));

    let poll = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/webhooks/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(poll.status(), StatusCode::ACCEPTED);

    let fulfill = |payload: String| {
        app.clone().oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/webhooks/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(payload))
                .unwrap(),
        )
    };
    let (first, second) = tokio::join!(
        fulfill(r#""first""#.to_string()),
        fulfill(r#""second""#.to_string())
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(
        [first.status(), second.status()]
            .iter()
            .filter(|status| **status == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [first.status(), second.status()]
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );

    let poll = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/webhooks/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(poll.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(poll.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(payload == "first" || payload == "second");
    let matching = serde_json::to_string(&payload).unwrap();
    let conflicting = if payload == "first" {
        r#""second""#
    } else {
        r#""first""#
    };
    assert_eq!(fulfill(matching).await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        fulfill(conflicting.to_string()).await.unwrap().status(),
        StatusCode::CONFLICT
    );

    let too_large = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/webhooks/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"payload":"{}"}}"#,
                    "x".repeat(256)
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(too_large.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let expired = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/callbacks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let expired: Value =
        serde_json::from_slice(&to_bytes(expired.into_body(), usize::MAX).await.unwrap()).unwrap();
    let expired_id = expired["id"].as_str().unwrap();
    sqlx::query("UPDATE oneoff_hooks SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
        .bind(Uuid::parse_str(expired_id).unwrap())
        .execute(&state.db)
        .await
        .unwrap();
    let poll_expired = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/webhooks/{expired_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(poll_expired.status(), StatusCode::NOT_FOUND);
    let fulfill_expired = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/webhooks/{expired_id}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"late":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fulfill_expired.status(), StatusCode::NOT_FOUND);
    assert!(cleanup_expired_callbacks(&state.db).await.unwrap() >= 1);
    let expired_remaining =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oneoff_hooks WHERE id = $1")
            .bind(Uuid::parse_str(expired_id).unwrap())
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(expired_remaining, 0);
    let remaining = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oneoff_hooks WHERE id = $1")
        .bind(Uuid::parse_str(id).unwrap())
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(remaining, 1);
    sqlx::query("DELETE FROM oneoff_hooks WHERE id = $1")
        .bind(Uuid::parse_str(id).unwrap())
        .execute(&state.db)
        .await
        .unwrap();
}

async fn send_json(
    app: &Router,
    method: Method,
    uri: String,
    body: &str,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request should build"),
        )
        .await
        .expect("response should be returned")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    serde_json::from_slice(&body).expect("response body should be JSON")
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database in TEST_DATABASE_URL"]
async fn webhook_consumer_and_operator_endpoints_follow_lifecycle() {
    let _database_guard = TEST_DATABASE_LOCK.lock().await;
    let mut state = test_state();
    state.db = PgPool::connect(&env::var("TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    state.sensitive_headers = parse_sensitive_keys_config("authorization");
    state.sensitive_query_keys = parse_sensitive_keys_config("api_key");
    ensure_schema(&state.db).await.unwrap();
    let app = build_app(state.clone());
    let tenant = Uuid::new_v4().to_string();
    let app_name = "lifecycle-test";
    let event = "created";

    let capture = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/api/webhooks/{tenant}/{app_name}/{event}?api_key=private&source=test"
                ))
                .header("content-type", "application/json")
                .header("authorization", "Bearer private")
                .body(Body::from(r#"{"value":1}"#))
                .expect("request should build"),
        )
        .await
        .expect("response should be returned");
    assert_eq!(capture.status(), StatusCode::ACCEPTED);
    let capture_body = response_json(capture).await;
    let id = capture_body["id"].as_str().unwrap();

    let peek = send_json(
        &app,
        Method::GET,
        format!("/api/consumer/peek/{tenant}/{app_name}/{event}"),
        "",
    )
    .await;
    assert_eq!(peek.status(), StatusCode::OK);
    let peek_body = response_json(peek).await;
    assert_eq!(peek_body["id"], id);
    assert_eq!(peek_body["status"], "new");
    assert_eq!(peek_body["headers"]["authorization"][0], "[REDACTED]");
    assert_eq!(peek_body["query_params"]["api_key"], "[REDACTED]");

    let active = send_json(
        &app,
        Method::GET,
        format!("/api/operator/active-webhooks?tenant={tenant}&app={app_name}"),
        "",
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let active_body = response_json(active).await;
    assert_eq!(active_body[0]["pending_new"], 1);

    let receive = send_json(
        &app,
        Method::POST,
        format!("/api/consumer/receive/{tenant}/{app_name}/{event}?ttl_seconds=30"),
        "",
    )
    .await;
    assert_eq!(receive.status(), StatusCode::OK);
    let receive_body = response_json(receive).await;
    assert_eq!(receive_body["id"], id);
    assert_eq!(receive_body["status"], "received");
    assert_eq!(receive_body["active"], true);

    let check_in = send_json(
        &app,
        Method::POST,
        format!("/api/consumer/webhooks/{id}/check-in"),
        r#"{"status_text":"working","intermediate_status":"stage-1"}"#,
    )
    .await;
    assert_eq!(check_in.status(), StatusCode::OK);
    let check_in_body = response_json(check_in).await;
    assert_eq!(check_in_body["status_text"], "working");
    assert_eq!(check_in_body["intermediate_status"], "stage-1");

    let complete = send_json(
        &app,
        Method::POST,
        format!("/api/consumer/webhooks/{id}/complete"),
        r#"{"outcome":"success","result":{"ok":true},"extra_properties":{"attempt":1}}"#,
    )
    .await;
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_body = response_json(complete).await;
    assert_eq!(complete_body["status"], "success");
    assert_eq!(complete_body["active"], false);
    assert_eq!(complete_body["result"]["ok"], true);

    let status = send_json(
        &app,
        Method::GET,
        format!("/api/operator/webhooks/{id}/status"),
        "",
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(response_json(status).await["status"], "success");

    let summary = send_json(&app, Method::GET, "/api/operator/status".to_string(), "").await;
    assert_eq!(summary.status(), StatusCode::OK);
    assert!(response_json(summary).await["total"].as_i64().unwrap() > 0);

    sqlx::query("DELETE FROM incoming_webhooks WHERE tenant = $1 AND app = $2")
        .bind(&tenant)
        .bind(app_name)
        .execute(&state.db)
        .await
        .unwrap();
}

#[test]
fn webhook_dimensions_extracts_tenant_app_event() {
    let (tenant, app, event) = webhook_dimensions("/api/webhooks/tenantA/appB/eventC");

    assert_eq!(tenant.as_deref(), Some("tenantA"));
    assert_eq!(app.as_deref(), Some("appB"));
    assert_eq!(event.as_deref(), Some("eventC"));
}

#[test]
fn webhook_dimensions_handles_tenant_app_without_event() {
    let (tenant, app, event) = webhook_dimensions("/api/webhooks/tenantA/appB");

    assert_eq!(tenant.as_deref(), Some("tenantA"));
    assert_eq!(app.as_deref(), Some("appB"));
    assert_eq!(event, None);
}

#[test]
fn webhook_dimensions_ignores_extra_segments() {
    let (tenant, app, event) = webhook_dimensions("/api/webhooks/t/a/e/extra/path");

    assert_eq!(tenant.as_deref(), Some("t"));
    assert_eq!(app.as_deref(), Some("a"));
    assert_eq!(event.as_deref(), Some("e"));
}

#[test]
fn webhook_dimensions_handles_base_path() {
    let (tenant, app, event) = webhook_dimensions("/api/webhooks");

    assert_eq!(tenant, None);
    assert_eq!(app, None);
    assert_eq!(event, None);
}

#[test]
fn resolve_ttl_uses_default_when_absent() {
    let ttl = resolve_ttl_seconds(None, 300).expect("ttl should resolve from default");
    assert_eq!(ttl, 300);
}

#[test]
fn resolve_ttl_prefers_request_value() {
    let ttl = resolve_ttl_seconds(Some(45), 300).expect("ttl should resolve from request");
    assert_eq!(ttl, 45);
}

#[test]
fn resolve_ttl_rejects_non_positive_values() {
    let err = resolve_ttl_seconds(Some(0), 300).expect_err("ttl=0 must fail");
    assert_eq!(err, StatusCode::BAD_REQUEST);
}

#[test]
fn sensitive_key_matching_supports_exact_suffix_and_contains() {
    let config = SensitiveKeysConfig {
        exact_matches: vec!["authorization".to_string()],
        contains: vec!["secret".to_string()],
        suffix: vec!["token".to_string()],
    };

    assert!(is_sensitive_key("authorization", &config));
    assert!(is_sensitive_key("refresh_token", &config));
    assert!(is_sensitive_key("my_secret_value", &config));
    assert!(!is_sensitive_key("x-request-id", &config));
}

#[test]
fn sensitive_key_config_parses_trimmed_case_insensitive_patterns() {
    let config = parse_sensitive_keys_config(" Authorization , *Secret* , *TOKEN, ,");
    assert!(is_sensitive_key("authorization", &config));
    assert!(is_sensitive_key("client-secret-value", &config));
    assert!(is_sensitive_key("refresh_token", &config));
    assert!(!is_sensitive_key("request-id", &config));
}

#[test]
fn headers_and_query_values_are_redacted_before_storage() {
    let config = parse_sensitive_keys_config("authorization,*secret*,*token");
    let mut headers = HeaderMap::new();
    headers.append("authorization", "Bearer secret".parse().unwrap());
    headers.append("x-request-id", "request-1".parse().unwrap());
    headers.append("x-request-id", "request-2".parse().unwrap());
    let stored_headers = headers_to_json(&headers, &config);
    assert_eq!(stored_headers["authorization"][0], "[REDACTED]");
    assert_eq!(
        stored_headers["x-request-id"],
        serde_json::json!(["request-1", "request-2"])
    );

    let query = HashMap::from([
        ("access_token".to_string(), "secret-token".to_string()),
        ("source".to_string(), "test".to_string()),
    ]);
    let stored_query = query_to_json(&query, &config);
    assert_eq!(stored_query["access_token"], "[REDACTED]");
    assert_eq!(stored_query["source"], "test");
}

#[test]
fn base64_encoding_handles_empty_and_each_padding_length() {
    assert_eq!(encode_base64(b""), "");
    assert_eq!(encode_base64(b"f"), "Zg==");
    assert_eq!(encode_base64(b"fo"), "Zm8=");
    assert_eq!(encode_base64(b"foo"), "Zm9v");
    assert_eq!(encode_base64(&[0, 255]), "AP8=");
}

#[test]
fn mixed_case_http_method_is_not_post() {
    let mixed_case = Method::from_bytes(b"Post").expect("method token should parse");
    assert_ne!(mixed_case, Method::POST);
}

#[tokio::test]
async fn operator_summary_rejects_post_method() {
    let app = test_app();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/operator/status")
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .oneshot(request)
        .await
        .expect("response should be returned");
    assert_eq!(response.status(), HttpStatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn operator_webhook_status_rejects_non_uuid_ids() {
    let app = test_app();
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/operator/webhooks/not-a-uuid/status")
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .oneshot(request)
        .await
        .expect("response should be returned");
    assert_eq!(response.status(), HttpStatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn operator_active_webhooks_rejects_post_method() {
    let app = test_app();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/operator/active-webhooks")
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .oneshot(request)
        .await
        .expect("response should be returned");
    assert_eq!(response.status(), HttpStatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn operator_active_webhooks_rejects_app_filter_without_tenant() {
    let app = test_app();
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/operator/active-webhooks?app=orders")
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .oneshot(request)
        .await
        .expect("response should be returned");
    assert_eq!(response.status(), HttpStatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn consumer_receive_rejects_get_method() {
    let app = test_app();
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/consumer/receive/tenant/app")
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .oneshot(request)
        .await
        .expect("response should be returned");
    assert_eq!(response.status(), HttpStatusCode::METHOD_NOT_ALLOWED);
}
