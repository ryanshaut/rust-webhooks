use super::*;

pub(super) async fn operator_status(State(state): State<AppState>) -> Response {
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

pub(super) async fn operator_webhook_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Response {
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

pub(super) async fn operator_active_webhooks(
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
