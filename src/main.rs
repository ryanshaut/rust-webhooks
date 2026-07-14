use axum::{
    body::Bytes,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::any,
    Json, Router,
};
use chrono::{DateTime, Utc};
use dotenvy::dotenv;
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{collections::HashMap, env, net::SocketAddr};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    sensitive_headers: SensitiveKeysConfig,
    sensitive_query_keys: SensitiveKeysConfig,
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

    let sensitive_headers = load_sensitive_keys_config("SENSITIVE_HEADERS", DEFAULT_SENSITIVE_HEADERS);
    let sensitive_query_keys = load_sensitive_keys_config("SENSITIVE_QUERY_KEYS", DEFAULT_SENSITIVE_QUERY_KEYS);

    let app = Router::new()
        .route("/api/webhooks", any(capture_webhook))
        .route("/api/webhooks/{*rest}", any(capture_webhook))
        .with_state(AppState { db, sensitive_headers, sensitive_query_keys });

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind tcp listener");

    println!("Webhook server listening on http://{}", addr);

    axum::serve(listener, app)
        .await
        .expect("server failed");
}

async fn ensure_schema(db: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS incoming_webhooks (
            id UUID PRIMARY KEY,
            received_at TIMESTAMPTZ NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            query_params JSONB NOT NULL,
            headers JSONB NOT NULL,
            body_text TEXT,
            body_base64 TEXT NOT NULL
        );
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
            query_params,
            headers,
            body_text,
            body_base64
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(id)
    .bind(received_at)
    .bind(method.to_string())
    .bind(original_uri.path().to_string())
    .bind(query_json)
    .bind(headers_json)
    .bind(body_text)
    .bind(body_base64)
    .execute(&state.db)
    .await;

    match insert_result {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(WebhookAcceptedResponse { id, received_at }),
        )
            .into_response(),
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
                map.insert(
                    key,
                    Value::Array(vec![Value::String(stored_value)]),
                );
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
