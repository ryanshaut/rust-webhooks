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

### WebSocket receive

- `GET /api/consumer/ws/{tenant}/{app}?ttl_seconds=300` (WebSocket upgrade)
- `GET /api/consumer/ws/{tenant}/{app}/{event}?ttl_seconds=300` (WebSocket upgrade)

For example, connect to `ws://localhost:3000/api/consumer/ws/shopify/orders`.
Topic matching is identical to HTTP receive: omitting `event` matches all events
for that tenant/app; providing it matches that exact event.

The server sends one JSON text message per webhook, using the same record format
and claim/TTL lifecycle as HTTP receive. Pending webhooks are delivered immediately
on connection, and newly captured webhooks trigger delivery without database polling.
Clients do not need to send receive requests over the socket.

An active subscription reserves its topic on this server instance:

- Another WebSocket connection with an overlapping topic returns `409 Conflict`
  before upgrade (including exact-event vs. all-event overlaps in either direction).
- HTTP receive for an overlapping topic also returns `409 Conflict`.
- Non-overlapping subscriptions and HTTP receives remain available. Peek, complete,
  check-in, and operator endpoints remain HTTP-only and are not blocked.

Closing or losing the connection releases the reservation. Already delivered records
remain `received` until completed or expired; use the existing HTTP complete/check-in
endpoints for them. Invalid non-positive TTLs return `400 Bad Request` before upgrade.
Database errors send an `{"error":"failed to receive webhook"}` message and end the
connection. Slow clients whose sends time out are disconnected.

Reservations and notifications are in-process: WebSocket subscribers and their webhook
producers must use the same server instance. They do not coordinate across replicas
or detect inserts made directly into PostgreSQL.

WebSocket conflict tests run with `cargo test websocket`. To also test delivery
against a disposable PostgreSQL database, set `TEST_DATABASE_URL` and run:

```bash
cargo test websocket_delivers -- --ignored
```

#### Python producer and consumer examples

The examples follow `scripts/test.py`: webhooks are produced over HTTP,
received over WebSocket, and completed over HTTP. With the server running,
install the Python dependencies from the repository root:

```bash
uv sync
```

Start the consumer in one terminal:

```bash
uv run python scripts/websocket_consumer.py --topic test/foo --count 5
```

Then send five sample webhooks from another terminal:

```bash
uv run python scripts/websocket_producer.py --topic test/foo/buzz --count 5
```

The consumer prints each record and marks it successful. Omitting `--count` keeps
it listening until Ctrl+C. Use `--topic test/foo/buzz` to subscribe to one exact
event instead of all events. Both scripts accept `--base-url http://localhost:3000`;
the consumer converts HTTPS URLs to `wss://` automatically. The producer accepts
`--interval` (seconds between requests), and the consumer accepts `--ttl-seconds`.
An overlapping consumer fails with HTTP `409 Conflict`; these examples do not
retry or perform long-running processing/check-ins.

### HTTP lifecycle and operator endpoints

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

- `GET /api/operator/active-webhooks`

Read-only operator endpoint for orchestration (for example, Airflow). Returns all active webhook streams grouped by `tenant/app/event` with counts for:

- `pending_new`: active webhooks that have not been claimed yet (`status = new`)
- `in_flight_received`: active webhooks currently claimed by consumers (`status = received`)
- `total_active`: total active webhooks in the stream

Only streams with at least one `pending_new` webhook are returned.

Optional query filters:

- `tenant`: return only streams for a tenant
- `tenant` + `app`: return only streams for a specific tenant/app pair

Examples:

- `/api/operator/active-webhooks?tenant=shopify`
- `/api/operator/active-webhooks?tenant=shopify&app=orders`

If `app` is provided without `tenant`, the endpoint returns `400 Bad Request`.

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
