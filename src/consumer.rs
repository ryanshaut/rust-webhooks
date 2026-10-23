use super::*;

fn validate_topic(
    tenant: &str,
    app: &str,
    event: Option<&str>,
) -> Result<(), (StatusCode, Json<Value>)> {
    let mut wildcard = false;
    for segment in [Some(tenant), Some(app), event].into_iter().flatten() {
        if segment == "*" {
            wildcard = true;
        } else if wildcard {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "wildcards must be trailing: no literal segment may follow '*'"
                })),
            ));
        }
    }
    Ok(())
}

pub(super) async fn peek_webhook_tenant_app(
    State(state): State<AppState>,
    Path((tenant, app)): Path<(String, String)>,
) -> Response {
    peek_webhook(&state, &tenant, &app, None).await
}

pub(super) async fn peek_webhook_tenant_app_event(
    State(state): State<AppState>,
    Path((tenant, app, event)): Path<(String, String, String)>,
) -> Response {
    peek_webhook(&state, &tenant, &app, Some(&event)).await
}

pub(super) async fn receive_webhook_tenant_app(
    State(state): State<AppState>,
    Path((tenant, app)): Path<(String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
) -> Response {
    receive_webhook(&state, &tenant, &app, None, ttl_query.ttl_seconds).await
}

pub(super) async fn receive_webhook_tenant_app_event(
    State(state): State<AppState>,
    Path((tenant, app, event)): Path<(String, String, String)>,
    Query(ttl_query): Query<TtlQueryParams>,
) -> Response {
    receive_webhook(&state, &tenant, &app, Some(&event), ttl_query.ttl_seconds).await
}

pub(super) async fn websocket_receive_tenant_app(
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

pub(super) async fn websocket_receive_tenant_app_event(
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

pub(super) async fn websocket_receive(
    state: AppState,
    topic: WebhookTopic,
    ttl_query: TtlQueryParams,
    upgrade: WebSocketUpgrade,
) -> Response {
    if let Err(error) = validate_topic(&topic.tenant, &topic.app, topic.event.as_deref()) {
        return error.into_response();
    }
    let ttl_seconds = match resolve_ttl_seconds(ttl_query.ttl_seconds, state.default_ttl_seconds) {
        Ok(ttl) => ttl,
        Err(status) => {
            return (
                status,
                Json(serde_json::json!({ "error": "ttl_seconds must be greater than 0" })),
            )
                .into_response();
        }
    };
    let subscription = match state.subscriptions.register(topic.clone()).await {
        Ok(subscription) => subscription,
        Err(()) => return subscription_conflict(),
    };
    let changes = state.webhooks_changed.subscribe();
    upgrade
        .max_message_size(1024)
        .max_frame_size(1024)
        .on_upgrade(move |socket| {
            websocket_receive_loop(socket, state, topic, ttl_seconds, changes, subscription)
        })
}

pub(super) async fn websocket_receive_loop(
    mut socket: WebSocket,
    state: AppState,
    topic: WebhookTopic,
    ttl_seconds: i64,
    mut changes: watch::Receiver<()>,
    _subscription: TopicReservation,
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

pub(super) async fn complete_webhook(
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

pub(super) async fn check_in_webhook(
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
        Err(status) => {
            return (
                status,
                Json(serde_json::json!({ "error": "ttl_seconds must be greater than 0" })),
            )
                .into_response();
        }
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

pub(super) async fn peek_webhook(
    state: &AppState,
    tenant: &str,
    app: &str,
    event: Option<&str>,
) -> Response {
    if let Err(error) = validate_topic(tenant, app, event) {
        return error.into_response();
    }
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
        WHERE ($1 = '*' OR tenant = $1)
          AND ($2 = '*' OR app = $2)
          AND ($3::text IS NULL OR $3 = '*' OR event = $3)
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

pub(super) async fn receive_webhook(
    state: &AppState,
    tenant: &str,
    app: &str,
    event: Option<&str>,
    requested_ttl_seconds: Option<i64>,
) -> Response {
    if let Err(error) = validate_topic(tenant, app, event) {
        return error.into_response();
    }
    let topic = WebhookTopic {
        tenant: tenant.to_string(),
        app: app.to_string(),
        event: event.map(str::to_string),
    };
    let _reservation = match state.subscriptions.reserve(topic, false) {
        Ok(reservation) => reservation,
        Err(_) => return subscription_conflict(),
    };
    match claim_webhook(state, tenant, app, event, requested_ttl_seconds).await {
        Ok(Some(webhook)) => (StatusCode::OK, Json(webhook)).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(response) => *response,
    }
}

pub(super) async fn claim_webhook(
    state: &AppState,
    tenant: &str,
    app: &str,
    event: Option<&str>,
    requested_ttl_seconds: Option<i64>,
) -> Result<Option<StoredWebhookRecord>, Box<Response>> {
    expire_stale_claims(&state.db).await.map_err(Box::new)?;

    let ttl_seconds = resolve_ttl_seconds(requested_ttl_seconds, state.default_ttl_seconds)
        .map_err(|status| {
            Box::new(
                (
                    status,
                    Json(serde_json::json!({ "error": "ttl_seconds must be greater than 0" })),
                )
                    .into_response(),
            )
        })?;

    let claimed = sqlx::query_as::<_, StoredWebhookRecord>(
        r#"
        WITH candidate AS (
            SELECT id
            FROM incoming_webhooks
            WHERE ($1 = '*' OR tenant = $1)
              AND ($2 = '*' OR app = $2)
              AND ($3::text IS NULL OR $3 = '*' OR event = $3)
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

pub(super) async fn expire_stale_claims(db: &PgPool) -> Result<(), Response> {
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
