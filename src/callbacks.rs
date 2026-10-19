use super::*;

pub(super) async fn rate_limit_callbacks(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let is_callback_create =
        request.uri().path() == "/api/callbacks" && request.method() == Method::POST;
    let is_callback_delivery = callback_webhook_id(request.uri().path()).is_some()
        && matches!(request.method(), &Method::GET | &Method::POST);
    let route_is_callback = is_callback_create || is_callback_delivery;
    if !route_is_callback {
        return next.run(request).await;
    }
    let ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(address)| address.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    if !allow_callback_request(&state, ip, Instant::now()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "60")],
            Json(serde_json::json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
    }
    next.run(request).await
}

pub(super) async fn api_webhook(
    State(state): State<AppState>,
    method: Method,
    original_uri: OriginalUri,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if let Some(id) = callback_webhook_id(original_uri.path()) {
        match method {
            Method::GET => return poll_callback(State(state), Path(id)).await,
            Method::POST => {
                if body.len() > state.callback_max_body_bytes {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(serde_json::json!({ "error": "request body is too large" })),
                    )
                        .into_response();
                }
                return fulfill_callback(State(state), Path(id), body).await;
            }
            _ => {}
        }
    }
    webhooks::capture_webhook(
        State(state),
        method,
        original_uri,
        headers,
        Query(query),
        body,
    )
    .await
    .into_response()
}

pub(super) fn callback_webhook_id(path: &str) -> Option<Uuid> {
    let id = path.strip_prefix("/api/webhooks/")?;
    if id.contains('/') {
        return None;
    }
    Uuid::parse_str(id).ok()
}

pub(super) fn allow_callback_request(state: &AppState, ip: IpAddr, now: Instant) -> bool {
    let window = Duration::from_secs(60);
    let mut rate_limits = state.callback_rate_limits.lock().unwrap();
    rate_limits.retain(|_, limit| now.duration_since(limit.started_at) < window * 2);
    let limit = rate_limits.entry(ip).or_insert(RateLimitWindow {
        started_at: now,
        requests: 0,
    });
    if now.duration_since(limit.started_at) >= window {
        limit.started_at = now;
        limit.requests = 0;
    }
    if limit.requests >= state.callback_rate_limit_per_minute {
        return false;
    }
    limit.requests += 1;
    true
}

pub(super) async fn create_callback(State(state): State<AppState>) -> Response {
    let created = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO oneoff_hooks (expires_at)
        VALUES (NOW() + ($1::bigint * INTERVAL '1 second'))
        RETURNING id
        "#,
    )
    .bind(state.callback_pending_ttl_seconds)
    .fetch_one(&state.db)
    .await;

    match created {
        Ok(id) => (
            StatusCode::CREATED,
            Json(CallbackCreatedResponse {
                id,
                callback_url: format!("/api/webhooks/{id}"),
            }),
        )
            .into_response(),
        Err(err) => {
            eprintln!("failed to create callback: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to create callback" })),
            )
                .into_response()
        }
    }
}

pub(super) async fn poll_callback(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let hook = sqlx::query_as::<_, CallbackState>(
        r#"
        SELECT status, payload
        FROM oneoff_hooks
        WHERE id = $1 AND expires_at > NOW()
        "#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await;

    match hook {
        Ok(Some(hook)) if hook.status == "pending" => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "status": "pending" })),
        )
            .into_response(),
        Ok(Some(hook)) if hook.status == "fulfilled" => {
            Json(hook.payload.unwrap_or(Value::Null)).into_response()
        }
        Ok(Some(_)) => {
            eprintln!("callback has an invalid status");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            eprintln!("failed to poll callback: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to poll callback" })),
            )
                .into_response()
        }
    }
}

pub(super) async fn fulfill_callback(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Response {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "request body must be valid JSON" })),
            )
                .into_response();
        }
    };

    let fulfilled = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE oneoff_hooks
        SET status = 'fulfilled',
            payload = $2,
            fulfilled_at = NOW(),
            expires_at = NOW() + ($3::bigint * INTERVAL '1 second')
        WHERE id = $1 AND status = 'pending' AND expires_at > NOW()
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(&payload)
    .bind(state.callback_retention_seconds)
    .fetch_optional(&state.db)
    .await;

    match fulfilled {
        Ok(Some(id)) => (
            StatusCode::OK,
            Json(CallbackFulfilledResponse {
                id,
                status: "fulfilled",
            }),
        )
            .into_response(),
        Ok(None) => {
            let existing = sqlx::query_as::<_, CallbackState>(
                r#"
                SELECT status, payload
                FROM oneoff_hooks
                WHERE id = $1 AND expires_at > NOW()
                "#,
            )
            .bind(id)
            .fetch_optional(&state.db)
            .await;
            match existing {
                Ok(Some(existing))
                    if existing.status == "fulfilled"
                        && existing.payload.as_ref() == Some(&payload) =>
                {
                    (
                        StatusCode::OK,
                        Json(CallbackFulfilledResponse {
                            id,
                            status: "fulfilled",
                        }),
                    )
                        .into_response()
                }
                Ok(Some(existing)) if existing.status == "fulfilled" => (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "callback already fulfilled" })),
                )
                    .into_response(),
                Ok(_) => StatusCode::NOT_FOUND.into_response(),
                Err(err) => {
                    eprintln!("failed to check callback fulfillment: {err}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "failed to fulfill callback" })),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            eprintln!("failed to fulfill callback: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to fulfill callback" })),
            )
                .into_response()
        }
    }
}

pub(super) async fn cleanup_expired_callbacks(db: &PgPool) -> Result<u64, sqlx::Error> {
    let deleted = sqlx::query(
        r#"
        WITH expired AS (
            SELECT id
            FROM oneoff_hooks
            WHERE expires_at <= NOW()
            ORDER BY expires_at
            LIMIT 1000
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM oneoff_hooks hook
        USING expired
        WHERE hook.id = expired.id
        "#,
    )
    .execute(db)
    .await?;
    Ok(deleted.rows_affected())
}
