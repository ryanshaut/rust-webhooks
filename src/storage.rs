use super::*;

pub(super) async fn ensure_schema(db: &PgPool) -> Result<(), sqlx::Error> {
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
        CREATE TABLE IF NOT EXISTS oneoff_hooks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'fulfilled')),
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            fulfilled_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ NOT NULL
        );
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS oneoff_hooks_expires_at_idx ON oneoff_hooks (expires_at)",
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
