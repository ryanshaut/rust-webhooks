use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, OriginalUri, Path, Query, State,
    },
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
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
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Notify};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    sensitive_headers: SensitiveKeysConfig,
    sensitive_query_keys: SensitiveKeysConfig,
    default_ttl_seconds: i64,
    callback_pending_ttl_seconds: i64,
    callback_retention_seconds: i64,
    callback_max_body_bytes: usize,
    callback_rate_limit_per_minute: u32,
    callback_rate_limits: Arc<Mutex<HashMap<IpAddr, RateLimitWindow>>>,
    subscriptions: Arc<WebsocketSubscriptions>,
    webhooks_changed: watch::Sender<()>,
}

#[derive(Clone, Copy)]
struct RateLimitWindow {
    started_at: Instant,
    requests: u32,
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
    topics: Mutex<HashMap<Uuid, ReservedTopic>>,
    changed: Notify,
}

struct ReservedTopic {
    topic: WebhookTopic,
    websocket: bool,
}

#[derive(Debug)]
enum ReservationConflict {
    Websocket,
    Http,
}

impl WebsocketSubscriptions {
    #[cfg(test)]
    fn conflicts(&self, topic: &WebhookTopic) -> bool {
        self.topics
            .lock()
            .unwrap()
            .values()
            .any(|existing| existing.websocket && existing.topic.overlaps(topic))
    }

    async fn register(self: &Arc<Self>, topic: WebhookTopic) -> Result<TopicReservation, ()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            // Register the waiter before checking claims to avoid missed releases.
            changed.as_mut().enable();
            match self.reserve(topic.clone(), true) {
                Ok(reservation) => return Ok(reservation),
                Err(ReservationConflict::Websocket) => return Err(()),
                Err(ReservationConflict::Http) => changed.await,
            }
        }
    }

    fn reserve(
        self: &Arc<Self>,
        topic: WebhookTopic,
        websocket: bool,
    ) -> Result<TopicReservation, ReservationConflict> {
        let mut topics = self.topics.lock().unwrap();
        if topics
            .values()
            .any(|existing| existing.websocket && existing.topic.overlaps(&topic))
        {
            return Err(ReservationConflict::Websocket);
        }
        if websocket
            && topics
                .values()
                .any(|existing| existing.topic.overlaps(&topic))
        {
            return Err(ReservationConflict::Http);
        }
        let id = Uuid::new_v4();
        topics.insert(id, ReservedTopic { topic, websocket });
        Ok(TopicReservation {
            registry: self.clone(),
            id,
        })
    }
}

struct TopicReservation {
    registry: Arc<WebsocketSubscriptions>,
    id: Uuid,
}

impl Drop for TopicReservation {
    fn drop(&mut self) {
        self.registry.topics.lock().unwrap().remove(&self.id);
        self.registry.changed.notify_waiters();
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

#[derive(Serialize)]
struct CallbackCreatedResponse {
    id: Uuid,
    callback_url: String,
}

#[derive(Serialize)]
struct CallbackFulfilledResponse {
    id: Uuid,
    status: &'static str,
}

#[derive(FromRow)]
struct CallbackState {
    status: String,
    payload: Option<Value>,
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

    storage::ensure_schema(&db)
        .await
        .expect("failed to ensure database schema");

    let sensitive_headers =
        webhooks::load_sensitive_keys_config("SENSITIVE_HEADERS", DEFAULT_SENSITIVE_HEADERS);
    let sensitive_query_keys =
        webhooks::load_sensitive_keys_config("SENSITIVE_QUERY_KEYS", DEFAULT_SENSITIVE_QUERY_KEYS);
    let default_ttl_seconds = env::var("DEFAULT_RECEIVE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);
    let callback_pending_ttl_seconds =
        positive_env_i64("CALLBACK_PENDING_TTL_SECONDS", 14 * 24 * 60 * 60);
    let callback_retention_seconds = positive_env_i64("CALLBACK_RETENTION_SECONDS", 72 * 60 * 60);
    let callback_max_body_bytes = positive_env_usize("CALLBACK_MAX_BODY_BYTES", 1024 * 1024);
    let callback_rate_limit_per_minute = positive_env_u32("CALLBACK_RATE_LIMIT_PER_MINUTE", 120);
    let cleanup_db = db.clone();

    let app = build_app(AppState {
        db,
        sensitive_headers,
        sensitive_query_keys,
        default_ttl_seconds,
        callback_pending_ttl_seconds,
        callback_retention_seconds,
        callback_max_body_bytes,
        callback_rate_limit_per_minute,
        callback_rate_limits: Arc::default(),
        subscriptions: Arc::default(),
        webhooks_changed: watch::channel(()).0,
    });

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(err) = callbacks::cleanup_expired_callbacks(&cleanup_db).await {
                eprintln!("failed to clean up expired callbacks: {err}");
            }
        }
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

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("server failed");
}

fn build_app(state: AppState) -> Router {
    let callback_routes = Router::new()
        .route("/api/callbacks", post(callbacks::create_callback))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            callbacks::rate_limit_callbacks,
        ));

    Router::new()
        .merge(callback_routes)
        .route("/api/webhooks", any(webhooks::capture_webhook))
        .route(
            "/api/webhooks/{*rest}",
            any(callbacks::api_webhook).layer(middleware::from_fn_with_state(
                state.clone(),
                callbacks::rate_limit_callbacks,
            )),
        )
        .route(
            "/api/consumer/peek/{tenant}/{app}",
            get(consumer::peek_webhook_tenant_app),
        )
        .route(
            "/api/consumer/peek/{tenant}/{app}/{event}",
            get(consumer::peek_webhook_tenant_app_event),
        )
        .route(
            "/api/consumer/receive/{tenant}/{app}",
            post(consumer::receive_webhook_tenant_app),
        )
        .route(
            "/api/consumer/receive/{tenant}/{app}/{event}",
            post(consumer::receive_webhook_tenant_app_event),
        )
        .route(
            "/api/consumer/ws/{tenant}/{app}",
            get(consumer::websocket_receive_tenant_app),
        )
        .route(
            "/api/consumer/ws/{tenant}/{app}/{event}",
            get(consumer::websocket_receive_tenant_app_event),
        )
        .route(
            "/api/consumer/webhooks/{id}/complete",
            post(consumer::complete_webhook),
        )
        .route(
            "/api/consumer/webhooks/{id}/check-in",
            post(consumer::check_in_webhook),
        )
        .route("/api/operator/status", get(operator::operator_status))
        .route(
            "/api/operator/active-webhooks",
            get(operator::operator_active_webhooks),
        )
        .route(
            "/api/operator/webhooks/{id}/status",
            get(operator::operator_webhook_status),
        )
        .with_state(state)
}

fn resolve_ttl_seconds(
    requested: Option<i64>,
    default_ttl_seconds: i64,
) -> Result<i64, StatusCode> {
    let ttl_seconds = requested.unwrap_or(default_ttl_seconds);
    if ttl_seconds <= 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    Ok(ttl_seconds)
}

const DEFAULT_SENSITIVE_HEADERS: &str = "authorization,proxy-authorization,cookie,set-cookie,x-api-key,api-key,x-auth-token,x-csrf-token,x-signature,stripe-signature,x-hub-signature,x-hub-signature-256,x-webhook-signature,x-amz-security-token";

const DEFAULT_SENSITIVE_QUERY_KEYS: &str = "auth,authorization,token,access_token,refresh_token,id_token,api_key,apikey,key,secret,client_secret,signature,sig,password,passwd,jwt";

fn positive_env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn positive_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn positive_env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
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

mod callbacks;
mod consumer;
mod operator;
mod storage;
mod webhooks;

#[cfg(test)]
mod tests;
