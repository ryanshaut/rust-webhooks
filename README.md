# rust-webhooks

[![CI](https://github.com/ryanshaut/rust-webhooks/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ryanshaut/rust-webhooks/actions/workflows/ci.yml)

A simple Rust webhook catcher API.

Container images are published to `ghcr.io/ryanshaut/rust-webhooks`.

## What it does

- Accepts any HTTP method on `/api/webhooks` and `/api/webhooks/*`
- Stores each request in Postgres with:
  - HTTP method
  - request path
  - webhook dimensions (`tenant`, `app`, `event`) parsed from path when present
  - query params (JSON)
  - headers (JSON)
  - body as UTF-8 text when possible
  - raw body as base64 for non-UTF8 payloads
  - delivery lifecycle fields: `status`, `active`, `substatus`, `ttl_expires_at`

## Configuration

The app reads these env vars (matching `.env.example`):

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_DATABASE`
- `DEFAULT_RECEIVE_TTL_SECONDS` (optional, default `300`)

By default, the server listens on `0.0.0.0:3000`.

### Sensitive keys redaction

Headers and query parameters with sensitive keys (auth, tokens, secrets, etc.) are automatically redacted to `[REDACTED]` before storage.

Override the default denylist via environment variables (comma-separated, case-insensitive):

```bash
# Custom header keys to redact
SENSITIVE_HEADERS=authorization,x-api-key,custom-token

# Custom query parameter keys to redact
SENSITIVE_QUERY_KEYS=api_key,access_token,custom_secret
```

Patterns supported:
- Exact match: `authorization` → matches exactly "authorization"
- Suffix match: `*token` → matches keys ending with "_token", "refresh_token", etc.
- Contains match: `*secret*` → matches any key containing "secret"

## Run

```bash
cargo run
```

## Quick start with published image

```bash
docker run --rm -p 3000:3000 \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=postgres \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_DATABASE=webhooks \
  ghcr.io/ryanshaut/rust-webhooks:latest
```

## Test with curl

```bash
curl -i -X POST "http://localhost:3000/api/webhooks/shopify/orders?source=test" \
  -H "Content-Type: application/json" \
  -H "X-App: demo" \
  -d '{"order_id":123,"status":"paid"}'
```

## Load test with k6

Run a simple load simulation against the webhook endpoint using Docker (no local k6 install needed):

```bash
make k6
```

Optional overrides:

```bash
BASE_URL=http://127.0.0.1:3000 VUS=50 DURATION=60s RUN_ID=smoke make k6
```

Quick smoke test:

```bash
make k6-smoke
```

## Database table

The table is auto-created on startup:

`incoming_webhooks`

## Consumer API (peek / receive / complete / check-in)

A "webhook" can be scoped by either:
- `tenant/app/event` (exactly one stream)
- `tenant/app` (all events for that tenant + app)

Incoming webhooks are inserted as:
- `status = new`
- `active = true`
- `substatus = null`

When a consumer receives a webhook, it moves to `status = received` and gets a TTL.
If the consumer never completes it before TTL expiration, it is marked:
- `status = expired`
- `active = false`
- `substatus = retry-ttl`

### Endpoints

- `GET /api/consumer/peek/{tenant}/{app}`
- `GET /api/consumer/peek/{tenant}/{app}/{event}`

Returns the next pending (`new`) webhook without claiming it.

- `POST /api/consumer/receive/{tenant}/{app}?ttl_seconds=300`
- `POST /api/consumer/receive/{tenant}/{app}/{event}?ttl_seconds=300`

Claims and returns the next pending webhook, moving it to `received` with TTL. If `ttl_seconds` is omitted, `DEFAULT_RECEIVE_TTL_SECONDS` is used.

- `POST /api/consumer/webhooks/{id}/complete`

Mark a previously received webhook as finished.

Request body:

```json
{
  "outcome": "success"
}
```

or

```json
{
  "outcome": "failed",
  "substatus": "retry-transient"
}
```

- `POST /api/consumer/webhooks/{id}/check-in?ttl_seconds=300`

Extends TTL for a currently `received` webhook.

- `GET /api/operator/status`

Read-only operator summary endpoint. Returns aggregate counts across lifecycle states without mutating webhook status.

- `GET /api/operator/webhooks/{id}/status`

Read-only operator endpoint that returns the lifecycle status for a specific webhook ID (the ID returned when the webhook was accepted).

## Grafana dashboards

Grafana is provisioned in [db.docker-compose.yml](/home/rshaut/projects/rust-webhooks/db.docker-compose.yml) with a Postgres datasource and three dashboards:

- `Webhook Overview`: throughput, top tenants, top apps, event mix, and a recent payload explorer table.
- `Webhook Dimensions Catalog`: all distinct tenants, apps, events, and tenant/app/event combinations present in the database.
- `Webhook Payload Detail`: request metadata plus full headers, query params, and payload body for a selected webhook ID.

Start the database, Adminer, and Grafana:

```bash
docker compose -f db.docker-compose.yml up -d
```

Open Grafana at `http://localhost:3000` and sign in with the default local credentials (`admin` / `admin` unless you override them). The dashboards are auto-loaded from [grafana/dashboards](/home/rshaut/projects/rust-webhooks/grafana/dashboards).

The overview dashboard derives backend dimensions from the webhook path only:

- `tenant`: first segment after `/api/webhooks`.
- `app`: second segment after `/api/webhooks`.
- `event`: third segment after `/api/webhooks`.
- `tenant`, `app`, `event` variables filter to one path-derived value or `All`.
- `payload_search`: optional free-text filter against request path and UTF-8 body text.

Expected path shape for monitoring is `/api/webhooks/{tenant}/{app}/{event}/...`.
