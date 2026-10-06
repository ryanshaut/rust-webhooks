use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        OriginalUri, Path, Query, State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{watch, RwLock};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    sensitive_headers: SensitiveKeysConfig,
    sensitive_query_keys: SensitiveKeysConfig,
    default_ttl_seconds: i64,
    subscriptions: Arc<WebsocketSubscriptions>,
    webhooks_changed: watch::Sender<()>,
}

#[derive(Clone, Debug)]
struct WebhookTopic {
    tenant: String,
    app: String,
    event: Option<String>,
}

impl WebhookTopic {
    fn overlaps(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.app == other.app
            && (self.event.is_none() || other.event.is_none() || self.event == other.event)
    }
}

#[derive(Default)]
struct WebsocketSubscriptions {
    topics: Mutex<HashMap<Uuid, WebhookTopic>>,
    // HTTP claims hold a read lock so registration cannot race a claim.
    receive_gate: RwLock<()>,
}

impl WebsocketSubscriptions {
    fn conflicts(&self, topic: &WebhookTopic) -> bool {
        self.topics
            .lock()
            .unwrap()
            .values()
            .any(|existing| existing.overlaps(topic))
    }

    fn register(self: &Arc<Self>, topic: WebhookTopic) -> Result<WebsocketSubscription, ()> {
        let mut topics = self.topics.lock().unwrap();
        if topics.values().any(|existing| existing.overlaps(&topic)) {
            return Err(());
        }
        let id = Uuid::new_v4();
        topics.insert(id, topic);
        Ok(WebsocketSubscription {
            registry: self.clone(),
            id,
        })
    }
}

struct WebsocketSubscription {
    registry: Arc<WebsocketSubscriptions>,
    id: Uuid,
}

impl Drop for WebsocketSubscription {
    fn drop(&mut self) {
        self.registry.topics.lock().unwrap().remove(&self.id);
    }
}

fn subscription_conflict() -> Response {
    (
        StatusCode::CONFLICT,
        Json(serde_json::json!({ "error": "an overlapping WebSocket receive subscription is active" })),
    )
        .into_response()
}

#[derive(Clone)]
struct SensitiveKeysConfig {
    exact_matches: Vec<String>,
    contains: Vec<String>,
    suffix: Vec<String>,
}

#[derive(Serialize)]
struct WebhookAcceptedResponse {
    id: Uuid,
    received_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct TtlQueryParams {
    ttl_seconds: Option<i64>,
}

#[derive(Deserialize)]
struct ActiveWebhookFilters {
    tenant: Option<String>,
    app: Option<String>,
}

#[derive(Deserialize)]
struct CompleteWebhookRequest {
    outcome: String,
    substatus: Option<String>,
    result: Option<Value>,
    extra_properties: Option<Value>,
}

#[derive(Deserialize)]
struct CheckInRequest {
    status_text: Option<String>,
    intermediate_status: Option<String>,
}

#[derive(Serialize, FromRow)]
struct StoredWebhookRecord {
    id: Uuid,
    received_at: DateTime<Utc>,
    method: String,
    path: String,
    tenant: Option<String>,
    app: Option<String>,
    event: Option<String>,
    query_params: Value,
    headers: Value,
    body_text: Option<String>,
    body_base64: String,
    status: String,
    active: bool,
    substatus: Option<String>,
    ttl_expires_at: Option<DateTime<Utc>>,
    intermediate_status: Option<String>,
    status_text: Option<String>,
    result: Option<Value>,
    extra_properties: Option<Value>,
}

#[derive(Serialize, FromRow)]
struct CompletionResponse {
    id: Uuid,
    status: String,
    active: bool,
    substatus: Option<String>,
    result: Option<Value>,
    extra_properties: Option<Value>,
}

#[derive(Serialize, FromRow)]
struct CheckInResponse {
    id: Uuid,
    status: String,
    ttl_expires_at: DateTime<Utc>,
    intermediate_status: Option<String>,
    status_text: Option<String>,
}

#[derive(Serialize, FromRow)]
struct OperatorStatusResponse {
    pending_new: i64,
    in_flight_received: i64,
    completed_success: i64,
    completed_failed: i64,
    expired_ttl: i64,
    total: i64,
}

#[derive(Serialize, FromRow)]
struct OperatorWebhookStatusResponse {
    id: Uuid,
    received_at: DateTime<Utc>,
    tenant: Option<String>,
    app: Option<String>,
    event: Option<String>,
    status: String,
    active: bool,
    substatus: Option<String>,
    ttl_expires_at: Option<DateTime<Utc>>,
    intermediate_status: Option<String>,
    status_text: Option<String>,
    result: Option<Value>,
    extra_properties: Option<Value>,
}

#[derive(Serialize, FromRow)]
struct OperatorActiveWebhookStreamResponse {
    tenant: String,
    app: String,
    event: String,
    pending_new: i64,
    in_flight_received: i64,
    total_active: i64,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let database_url = database_url_from_env();

    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("failed to connect to postgres");

    ensure_schema(&db)
        .await
        .expect("failed to ensure database schema");

    let sensitive_headers =
        load_sensitive_keys_config("SENSITIVE_HEADERS", DEFAULT_SENSITIVE_HEADERS);
    let sensitive_query_keys =
        load_sensitive_keys_config("SENSITIVE_QUERY_KEYS", DEFAULT_SENSITIVE_QUERY_KEYS);
    let default_ttl_seconds = env::var("DEFAULT_RECEIVE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);

    let app = build_app(AppState {
        db,
        sensitive_headers,
        sensitive_query_keys,
        default_ttl_seconds,
        subscriptions: Arc::default(),
        webhooks_changed: watch::channel(()).0,
    });

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind tcp listener");

    println!("Webhook server listening on http://{}", addr);

    axum::serve(listener, app).await.expect("server failed");
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/api/webhooks", any(capture_webhook))
        .route("/api/webhooks/{*rest}", any(capture_webhook))
        .route(
            "/api/consumer/peek/{tenant}/{app}",
            get(peek_webhook_tenant_app),
        )
        .route(
            "/api/consumer/peek/{tenant}/{app}/{event}",
            get(peek_webhook_tenant_app_event),
        )
        .route(
            "/api/consumer/receive/{tenant}/{app}",
            post(receive_webhook_tenant_app),
        )
        .route(
            "/api/consumer/receive/{tenant}/{app}/{event}",
            post(receive_webhook_tenant_app_event),
        )
        .route(
            "/api/consumer/ws/{tenant}/{app}",
            get(websocket_receive_tenant_app),
        )
        .route(
            "/api/consumer/ws/{tenant}/{app}/{event}",
            get(websocket_receive_tenant_app_event),
        )
        .route(
            "/api/consumer/webhooks/{id}/complete",
            post(complete_webhook),
        )
        .route(
            "/api/consumer/webhooks/{id}/check-in",
            post(check_in_webhook),
        )
        .route("/api/operator/status", get(operator_status))
        .route(
            "/api/operator/active-webhooks",
            get(operator_active_webhooks),
        )
        .route(
            "/api/operator/webhooks/{id}/status",
            get(operator_webhook_status),
        )
        .with_state(state)
}

async fn ensure_schema(db: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS incoming_webhooks (
            id UUID PRIMARY KEY,
            received_at TIMESTAMPTZ NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            tenant TEXT,
            app TEXT,
            event TEXT,
            query_params JSONB NOT NULL,
            headers JSONB NOT NULL,
            body_text TEXT,
            body_base64 TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            substatus TEXT,
            ttl_expires_at TIMESTAMPTZ
        );
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        r#"
        ALTER TABLE incoming_webhooks
            ADD COLUMN IF NOT EXISTS tenant TEXT,
            ADD COLUMN IF NOT EXISTS app TEXT,
            ADD COLUMN IF NOT EXISTS event TEXT,
            ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'new',
            ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE,
            ADD COLUMN IF NOT EXISTS substatus TEXT,
            ADD COLUMN IF NOT EXISTS ttl_expires_at TIMESTAMPTZ;
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        r#"
        ALTER TABLE incoming_webhooks
            ADD COLUMN IF NOT EXISTS intermediate_status TEXT,
            ADD COLUMN IF NOT EXISTS status_text TEXT,
            ADD COLUMN IF NOT EXISTS result JSONB,
            ADD COLUMN IF NOT EXISTS extra_properties JSONB;
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_incoming_webhooks_consumer_lookup
        ON incoming_webhooks (tenant, app, event, status, active, received_at);
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_incoming_webhooks_ttl
        ON incoming_webhooks (status, active, ttl_expires_at);
        "#,
    )
    .execute(db)
    .await?;

    // Backfill dimensions for legacy rows created before tenant/app/event columns were populated.
    sqlx::query(
        r#"
        UPDATE incoming_webhooks
        SET
            tenant = NULLIF(split_part(regexp_replace(path, '^/api/webhooks/?', ''), '/', 1), ''),
            app = NULLIF(split_part(regexp_replace(path, '^/api/webhooks/?', ''), '/', 2), ''),
            event = NULLIF(split_part(regexp_replace(path, '^/api/webhooks/?', ''), '/', 3), '')
        WHERE
            (tenant IS NULL OR app IS NULL OR event IS NULL)
            AND path LIKE '/api/webhooks%';
        "#,
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn capture_webhook(
    State(state): State<AppState>,
    method: Method,
    original_uri: OriginalUri,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> impl IntoResponse {
    let id = Uuid::new_v4();
    let received_at = Utc::now();
    let path = original_uri.path().to_string();
    let (tenant, app, event) = webhook_dimensions(&path);

    let headers_json = headers_to_json(&headers, &state.sensitive_headers);
    let query_json = query_to_json(&query, &state.sensitive_query_keys);

    let body_text = String::from_utf8(body.to_vec()).ok();
    let body_base64 = encode_base64(&body);

    let insert_result = sqlx::query(
        r#"
        INSERT INTO incoming_webhooks (
            id,
            received_at,
            method,
            path,
            tenant,
            app,
            event,
            query_params,
            headers,
            body_text,
            body_base64,
            status,
            active,
            substatus,
            ttl_expires_at,
            intermediate_status,
            status_text,
            result,
            extra_properties
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'new', TRUE, NULL, NULL, NULL, NULL, NULL, NULL)
        "#,
    )
    .bind(id)
    .bind(received_at)
    .bind(method.to_string())
    .bind(path)
    .bind(tenant)
    .bind(app)
    .bind(event)
    .bind(query_json)
    .bind(headers_json)
    .bind(body_text)
    .bind(body_base64)
    .execute(&state.db)
    .await;

    match insert_result {
        Ok(_) => {
            state.webhooks_changed.send_replace(());
            (
                StatusCode::ACCEPTED,
                Json(WebhookAcceptedResponse { id, received_at }),
            )
                .into_response()
        }
        Err(err) => {
            eprintln!("failed to persist webhook: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to persist webhook" })),
            )
                .into_response()
        }
    }
}

async fn peek_webhook_tenant_app(
    State(state): State<AppState>,
    Path((tenant, app)): Path<(String, String)>,
) -> Response {
    peek_webhook(&state, &tenant, &app, None).await
}

async fn peek_webhook_tenant_app_event(
    State(state): State<AppState>,
    Path((tenant, app, event)): Path<(String, String, String)>,
) -> Response {
    peek_webhook(&state, &tenant, &app, Some(&event)).await
}

async fn receive_webhook_tenant_app(
    State(state): State<AppState>,
    Path((tenant, app)): Path<(String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
) -> Response {
    receive_webhook(&state, &tenant, &app, None, ttl_query.ttl_seconds).await
}

async fn receive_webhook_tenant_app_event(
    State(state): State<AppState>,
    Path((tenant, app, event)): Path<(String, String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
) -> Response {
    receive_webhook(&state, &tenant, &app, Some(&event), ttl_query.ttl_seconds).await
}

async fn websocket_receive_tenant_app(
    State(state): State<AppState>,
    Path((tenant, app)): Path<(String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
    upgrade: WebSocketUpgrade,
) -> Response {
    websocket_receive(
        state,
        WebhookTopic {
            tenant,
            app,
            event: None,
        },
        ttl_query,
        upgrade,
    )
    .await
}

async fn websocket_receive_tenant_app_event(
    State(state): State<AppState>,
    Path((tenant, app, event)): Path<(String, String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
    upgrade: WebSocketUpgrade,
) -> Response {
    websocket_receive(
        state,
        WebhookTopic {
            tenant,
            app,
            event: Some(event),
        },
        ttl_query,
        upgrade,
    )
    .await
}

async fn websocket_receive(
    state: AppState,
    topic: WebhookTopic,
    ttl_query: TtlQueryParams,
    upgrade: WebSocketUpgrade,
) -> Response {
    let ttl_seconds = match resolve_ttl_seconds(ttl_query.ttl_seconds, state.default_ttl_seconds) {
        Ok(ttl) => ttl,
        Err(response) => return response,
    };
    let gate = state.subscriptions.receive_gate.write().await;
    let subscription = match state.subscriptions.register(topic.clone()) {
        Ok(subscription) => subscription,
        Err(()) => return subscription_conflict(),
    };
    let changes = state.webhooks_changed.subscribe();
    drop(gate);
    upgrade
        .max_message_size(1024)
        .max_frame_size(1024)
        .on_upgrade(move |socket| {
            websocket_receive_loop(socket, state, topic, ttl_seconds, changes, subscription)
        })
}

async fn websocket_receive_loop(
    mut socket: WebSocket,
    state: AppState,
    topic: WebhookTopic,
    ttl_seconds: i64,
    mut changes: watch::Receiver<()>,
    _subscription: WebsocketSubscription,
) {
    let mut pending = true;
    let mut claim = Box::pin(claim_webhook(
        &state,
        &topic.tenant,
        &topic.app,
        topic.event.as_deref(),
        Some(ttl_seconds),
    ));
    loop {
        let message = tokio::select! {
            biased;
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Ping(payload))) => Message::Pong(payload),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => continue,
            },
            claimed = &mut claim, if pending => {
                match claimed {
                    Ok(Some(webhook)) => {
                        claim = Box::pin(claim_webhook(
                            &state, &topic.tenant, &topic.app, topic.event.as_deref(), Some(ttl_seconds),
                        ));
                        Message::Text(serde_json::to_string(&webhook).unwrap().into())
                    }
                    Ok(None) => {
                        pending = false;
                        continue;
                    }
                    Err(_) => {
                        let _ = tokio::time::timeout(Duration::from_secs(10), socket.send(Message::Text(
                            serde_json::json!({ "error": "failed to receive webhook" }).to_string().into(),
                        ))).await;
                        break;
                    }
                }
            },
            changed = changes.changed(), if !pending => {
                if changed.is_err() {
                    break;
                }
                claim = Box::pin(claim_webhook(
                    &state, &topic.tenant, &topic.app, topic.event.as_deref(), Some(ttl_seconds),
                ));
                pending = true;
                continue;
            },
        };
        if !matches!(
            tokio::time::timeout(Duration::from_secs(10), socket.send(message)).await,
            Ok(Ok(()))
        ) {
            break;
        }
    }
}

async fn complete_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<CompleteWebhookRequest>,
) -> Response {
    if let Err(response) = expire_stale_claims(&state.db).await {
        return response;
    }

    let outcome = request.outcome.to_ascii_lowercase();
    if outcome != "success" && outcome != "failed" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "outcome must be either 'success' or 'failed'"
            })),
        )
            .into_response();
    }

    let final_substatus = if outcome == "success" {
        None
    } else {
        request.substatus
    };

    let result = match &request.result {
        Some(v) if !v.is_object() => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "result must be a JSON object" })),
            )
                .into_response();
        }
        other => other.clone(),
    };

    let extra_properties = match &request.extra_properties {
        Some(v) if !v.is_object() => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "extra_properties must be a JSON object" })),
            )
                .into_response();
        }
        other => other.clone(),
    };

    let updated = sqlx::query_as::<_, CompletionResponse>(
        r#"
        UPDATE incoming_webhooks
        SET status = $2, active = FALSE, substatus = $3, ttl_expires_at = NULL,
            result = $4, extra_properties = $5
        WHERE id = $1 AND status = 'received' AND active = TRUE
        RETURNING id, status, active, substatus, result, extra_properties
        "#,
    )
    .bind(id)
    .bind(outcome)
    .bind(final_substatus)
    .bind(result)
    .bind(extra_properties)
    .fetch_optional(&state.db)
    .await;

    match updated {
        Ok(Some(webhook)) => (StatusCode::OK, Json(webhook)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "webhook not found or not in 'received' state"
            })),
        )
            .into_response(),
        Err(err) => {
            eprintln!("failed to complete webhook {id}: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to complete webhook" })),
            )
                .into_response()
        }
    }
}

async fn check_in_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(ttl_query): Query<TtlQueryParams>,
    body: Option<Json<CheckInRequest>>,
) -> Response {
    if let Err(response) = expire_stale_claims(&state.db).await {
        return response;
    }

    let ttl_seconds = match resolve_ttl_seconds(ttl_query.ttl_seconds, state.default_ttl_seconds) {
        Ok(value) => value,
        Err(response) => return response,
    };

    let (status_text, intermediate_status) = match body {
        Some(Json(req)) => (req.status_text, req.intermediate_status),
        None => (None, None),
    };

    let updated = sqlx::query_as::<_, CheckInResponse>(
        r#"
        UPDATE incoming_webhooks
        SET ttl_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
            status_text = COALESCE($3, status_text),
            intermediate_status = COALESCE($4, intermediate_status)
        WHERE id = $1 AND status = 'received' AND active = TRUE
        RETURNING id, status, ttl_expires_at, intermediate_status, status_text
        "#,
    )
    .bind(id)
    .bind(ttl_seconds)
    .bind(status_text)
    .bind(intermediate_status)
    .fetch_optional(&state.db)
    .await;

    match updated {
        Ok(Some(webhook)) => (StatusCode::OK, Json(webhook)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "webhook not found or not in 'received' state"
            })),
        )
            .into_response(),
        Err(err) => {
            eprintln!("failed to check in webhook {id}: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to check in webhook" })),
            )
                .into_response()
        }
    }
}

async fn operator_status(State(state): State<AppState>) -> Response {
    let status = sqlx::query_as::<_, OperatorStatusResponse>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'new' AND active = TRUE) AS pending_new,
            COUNT(*) FILTER (WHERE status = 'received' AND active = TRUE) AS in_flight_received,
            COUNT(*) FILTER (WHERE status = 'success' AND active = FALSE) AS completed_success,
            COUNT(*) FILTER (WHERE status = 'failed' AND active = FALSE) AS completed_failed,
            COUNT(*) FILTER (WHERE status = 'expired') AS expired_ttl,
            COUNT(*) AS total
        FROM incoming_webhooks
        "#,
    )
    .fetch_one(&state.db)
    .await;

    match status {
        Ok(summary) => (StatusCode::OK, Json(summary)).into_response(),
        Err(err) => {
            eprintln!("failed to fetch operator status: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to fetch operator status" })),
            )
                .into_response()
        }
    }
}

async fn operator_webhook_status(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let webhook = sqlx::query_as::<_, OperatorWebhookStatusResponse>(
        r#"
        SELECT
            id,
            received_at,
            tenant,
            app,
            event,
            status,
            active,
            substatus,
            ttl_expires_at,
            intermediate_status,
            status_text,
            result,
            extra_properties
        FROM incoming_webhooks
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await;

    match webhook {
        Ok(Some(record)) => (StatusCode::OK, Json(record)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "webhook not found" })),
        )
            .into_response(),
        Err(err) => {
            eprintln!("failed to fetch webhook status for {id}: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to fetch webhook status" })),
            )
                .into_response()
        }
    }
}

async fn operator_active_webhooks(
    State(state): State<AppState>,
    Query(filters): Query<ActiveWebhookFilters>,
) -> Response {
    let ActiveWebhookFilters { tenant, app } = filters;

    if tenant.is_none() && app.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "tenant is required when app is provided"
            })),
        )
            .into_response();
    }

    let active_webhooks = sqlx::query_as::<_, OperatorActiveWebhookStreamResponse>(
        r#"
        SELECT
            tenant,
            app,
            event,
            COUNT(*) FILTER (WHERE status = 'new') AS pending_new,
            COUNT(*) FILTER (WHERE status = 'received') AS in_flight_received,
            COUNT(*) AS total_active
        FROM incoming_webhooks
        WHERE active = TRUE
          AND status IN ('new', 'received')
          AND tenant IS NOT NULL
          AND app IS NOT NULL
          AND event IS NOT NULL
                    AND ($1::text IS NULL OR tenant = $1)
                    AND ($2::text IS NULL OR app = $2)
        GROUP BY tenant, app, event
        HAVING COUNT(*) FILTER (WHERE status = 'new') > 0
        ORDER BY tenant ASC, app ASC, event ASC
        "#,
    )
    .bind(tenant)
    .bind(app)
    .fetch_all(&state.db)
    .await;

    match active_webhooks {
        Ok(records) => (StatusCode::OK, Json(records)).into_response(),
        Err(err) => {
            eprintln!("failed to fetch active webhooks: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to fetch active webhooks" })),
            )
                .into_response()
        }
    }
}

async fn peek_webhook(state: &AppState, tenant: &str, app: &str, event: Option<&str>) -> Response {
    if let Err(response) = expire_stale_claims(&state.db).await {
        return response;
    }

    let next = sqlx::query_as::<_, StoredWebhookRecord>(
        r#"
        SELECT
            id,
            received_at,
            method,
            path,
            tenant,
            app,
            event,
            query_params,
            headers,
            body_text,
            body_base64,
            status,
            active,
            substatus,
            ttl_expires_at,
            intermediate_status,
            status_text,
            result,
            extra_properties
        FROM incoming_webhooks
        WHERE tenant = $1
          AND app = $2
          AND ($3::text IS NULL OR event = $3)
          AND status = 'new'
          AND active = TRUE
        ORDER BY received_at ASC
        LIMIT 1
        "#,
    )
    .bind(tenant)
    .bind(app)
    .bind(event)
    .fetch_optional(&state.db)
    .await;

    match next {
        Ok(Some(webhook)) => (StatusCode::OK, Json(webhook)).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            eprintln!("failed to peek webhook: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to peek webhook" })),
            )
                .into_response()
        }
    }
}

async fn receive_webhook(
    state: &AppState,
    tenant: &str,
    app: &str,
    event: Option<&str>,
    requested_ttl_seconds: Option<i64>,
) -> Response {
    let _gate = state.subscriptions.receive_gate.read().await;
    let topic = WebhookTopic {
        tenant: tenant.to_string(),
        app: app.to_string(),
        event: event.map(str::to_string),
    };
    if state.subscriptions.conflicts(&topic) {
        return subscription_conflict();
    }
    match claim_webhook(state, tenant, app, event, requested_ttl_seconds).await {
        Ok(Some(webhook)) => (StatusCode::OK, Json(webhook)).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(response) => *response,
    }
}

async fn claim_webhook(
    state: &AppState,
    tenant: &str,
    app: &str,
    event: Option<&str>,
    requested_ttl_seconds: Option<i64>,
) -> Result<Option<StoredWebhookRecord>, Box<Response>> {
    expire_stale_claims(&state.db).await.map_err(Box::new)?;

    let ttl_seconds =
        resolve_ttl_seconds(requested_ttl_seconds, state.default_ttl_seconds).map_err(Box::new)?;

    let claimed = sqlx::query_as::<_, StoredWebhookRecord>(
        r#"
        WITH candidate AS (
            SELECT id
            FROM incoming_webhooks
            WHERE tenant = $1
              AND app = $2
              AND ($3::text IS NULL OR event = $3)
              AND status = 'new'
              AND active = TRUE
            ORDER BY received_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE incoming_webhooks webhook
        SET status = 'received',
            ttl_expires_at = NOW() + ($4::bigint * INTERVAL '1 second')
        FROM candidate
        WHERE webhook.id = candidate.id
        RETURNING
            webhook.id,
            webhook.received_at,
            webhook.method,
            webhook.path,
            webhook.tenant,
            webhook.app,
            webhook.event,
            webhook.query_params,
            webhook.headers,
            webhook.body_text,
            webhook.body_base64,
            webhook.status,
            webhook.active,
            webhook.substatus,
            webhook.ttl_expires_at,
            webhook.intermediate_status,
            webhook.status_text,
            webhook.result,
            webhook.extra_properties
        "#,
    )
    .bind(tenant)
    .bind(app)
    .bind(event)
    .bind(ttl_seconds)
    .fetch_optional(&state.db)
    .await;

    match claimed {
        Ok(webhook) => Ok(webhook),
        Err(err) => {
            eprintln!("failed to receive webhook: {err}");
            Err(Box::new(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "failed to receive webhook" })),
                )
                    .into_response(),
            ))
        }
    }
}

async fn expire_stale_claims(db: &PgPool) -> Result<(), Response> {
    let expired = sqlx::query(
        r#"
        UPDATE incoming_webhooks
        SET status = 'expired', active = FALSE, substatus = 'retry-ttl'
        WHERE status = 'received'
          AND active = TRUE
          AND ttl_expires_at IS NOT NULL
          AND ttl_expires_at <= NOW()
        "#,
    )
    .execute(db)
    .await;

    match expired {
        Ok(_) => Ok(()),
        Err(err) => {
            eprintln!("failed to expire stale claims: {err}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to update ttl status" })),
            )
                .into_response())
        }
    }
}

fn resolve_ttl_seconds(requested: Option<i64>, default_ttl_seconds: i64) -> Result<i64, Response> {
    let ttl_seconds = requested.unwrap_or(default_ttl_seconds);
    if ttl_seconds <= 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "ttl_seconds must be greater than 0" })),
        )
            .into_response());
    }

    Ok(ttl_seconds)
}

fn webhook_dimensions(path: &str) -> (Option<String>, Option<String>, Option<String>) {
    let normalized = path
        .trim_start_matches('/')
        .trim_start_matches("api/webhooks")
        .trim_start_matches('/');

    if normalized.is_empty() {
        return (None, None, None);
    }

    let mut parts = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(std::string::ToString::to_string);

    let tenant = parts.next();
    let app = parts.next();
    let event = parts.next();

    (tenant, app, event)
}

fn headers_to_json(headers: &HeaderMap, config: &SensitiveKeysConfig) -> Value {
    let mut map = Map::new();

    for (name, value) in headers {
        let key = name.to_string();
        let is_sensitive = is_sensitive_key(&key, config);

        if let Ok(value_str) = value.to_str() {
            let stored_value = if is_sensitive {
                "[REDACTED]".to_string()
            } else {
                value_str.to_string()
            };

            if let Some(existing) = map.get_mut(&key) {
                if let Some(arr) = existing.as_array_mut() {
                    arr.push(Value::String(stored_value));
                }
            } else {
                map.insert(key, Value::Array(vec![Value::String(stored_value)]));
            }
        }
    }

    Value::Object(map)
}

fn query_to_json(query: &HashMap<String, String>, config: &SensitiveKeysConfig) -> Value {
    let mut map = Map::new();

    for (key, value) in query {
        let stored_value = if is_sensitive_key(key, config) {
            "[REDACTED]".to_string()
        } else {
            value.clone()
        };

        map.insert(key.clone(), Value::String(stored_value));
    }

    Value::Object(map)
}

const DEFAULT_SENSITIVE_HEADERS: &str = "authorization,proxy-authorization,cookie,set-cookie,x-api-key,api-key,x-auth-token,x-csrf-token,x-signature,stripe-signature,x-hub-signature,x-hub-signature-256,x-webhook-signature,x-amz-security-token";

const DEFAULT_SENSITIVE_QUERY_KEYS: &str = "auth,authorization,token,access_token,refresh_token,id_token,api_key,apikey,key,secret,client_secret,signature,sig,password,passwd,jwt";

fn load_sensitive_keys_config(env_var: &str, default: &str) -> SensitiveKeysConfig {
    let csv = env::var(env_var).unwrap_or_else(|_| default.to_string());

    let mut exact_matches = Vec::new();
    let mut contains = Vec::new();
    let mut suffix = Vec::new();

    for key in csv.split(',') {
        let trimmed = key.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('*') && trimmed.ends_with('*') {
            contains.push(trimmed.trim_matches('*').to_string());
        } else if trimmed.ends_with('*') {
            suffix.push(trimmed.trim_end_matches('*').to_string());
        } else {
            exact_matches.push(trimmed);
        }
    }

    SensitiveKeysConfig {
        exact_matches,
        contains,
        suffix,
    }
}

fn is_sensitive_key(key: &str, config: &SensitiveKeysConfig) -> bool {
    let lower = key.to_ascii_lowercase();

    for exact in &config.exact_matches {
        if lower == *exact {
            return true;
        }
    }

    for pattern in &config.contains {
        if lower.contains(pattern) {
            return true;
        }
    }

    for prefix in &config.suffix {
        if lower.ends_with(prefix) {
            return true;
        }
    }

    false
}

fn encode_base64(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    let mut i = 0;

    while i + 3 <= data.len() {
        let b0 = data[i];
        let b1 = data[i + 1];
        let b2 = data[i + 2];

        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);

        i += 3;
    }

    let rem = data.len() - i;
    if rem == 1 {
        let b0 = data[i];
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[((b0 & 0b0000_0011) << 4) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b0 = data[i];
        let b1 = data[i + 1];
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        out.push(TABLE[((b1 & 0b0000_1111) << 2) as usize] as char);
        out.push('=');
    }

    out
}

fn database_url_from_env() -> String {
    let username = env::var("DB_USERNAME").unwrap_or_else(|_| "postgres".to_string());
    let password = env::var("DB_PASSWORD").unwrap_or_default();
    let host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string());
    let database = env::var("DB_DATABASE").unwrap_or_else(|_| "rust-webhooks".to_string());

    if password.is_empty() {
        format!("postgres://{username}@{host}:{port}/{database}")
    } else {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            urlencoding::encode(&username),
            urlencoding::encode(&password),
            host,
            port,
            database
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;
    use axum::{
        body::Body,
        http::{Request, StatusCode as HttpStatusCode},
    };
    use tower::ServiceExt;

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
            subscriptions: Arc::default(),
            webhooks_changed: watch::channel(()).0,
        }
    }

    fn test_app() -> Router {
        build_app(test_state())
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

    #[tokio::test]
    async fn websocket_registration_is_atomic_and_released_on_drop() {
        let registry = Arc::new(WebsocketSubscriptions::default());
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let registry = registry.clone();
            tasks.push(tokio::spawn(async move {
                registry.register(topic("tenant", "app", None)).ok()
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
        assert!(registry.register(topic("tenant", "app", None)).is_ok());
    }

    #[tokio::test]
    async fn websocket_registration_waits_for_in_flight_http_receive() {
        let registry = Arc::new(WebsocketSubscriptions::default());
        let claim_guard = registry.receive_gate.read().await;
        let subscriber_registry = registry.clone();
        let mut registration = tokio::spawn(async move {
            let _gate = subscriber_registry.receive_gate.write().await;
            subscriber_registry
                .register(topic("tenant", "app", None))
                .unwrap()
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut registration)
                .await
                .is_err()
        );
        assert!(!registry.conflicts(&topic("tenant", "app", None)));
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

        for scope in ["tenant/app/event", "tenant/app"] {
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
            tokio_tungstenite::connect_async(format!("{base}/api/consumer/ws/tenant/app"))
                .await
                .unwrap();
        replacement.close(None).await.unwrap();
        server.abort();
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
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
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
}
