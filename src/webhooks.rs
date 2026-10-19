use super::*;

pub(super) async fn capture_webhook(
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

pub(super) fn webhook_dimensions(path: &str) -> (Option<String>, Option<String>, Option<String>) {
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

pub(super) fn headers_to_json(headers: &HeaderMap, config: &SensitiveKeysConfig) -> Value {
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

pub(super) fn query_to_json(
    query: &HashMap<String, String>,
    config: &SensitiveKeysConfig,
) -> Value {
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

pub(super) fn load_sensitive_keys_config(env_var: &str, default: &str) -> SensitiveKeysConfig {
    let csv = env::var(env_var).unwrap_or_else(|_| default.to_string());
    parse_sensitive_keys_config(&csv)
}

pub(super) fn parse_sensitive_keys_config(csv: &str) -> SensitiveKeysConfig {
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
        } else if trimmed.starts_with('*') {
            suffix.push(trimmed.trim_start_matches('*').to_string());
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

pub(super) fn is_sensitive_key(key: &str, config: &SensitiveKeysConfig) -> bool {
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

pub(super) fn encode_base64(data: &[u8]) -> String {
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
