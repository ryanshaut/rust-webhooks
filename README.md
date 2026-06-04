# rust-webhooks

A simple Rust webhook catcher API.

## What it does

- Accepts any HTTP method on `/api/webhooks` and `/api/webhooks/*`
- Stores each request in Postgres with:
  - HTTP method
  - request path
  - query params (JSON)
  - headers (JSON)
  - body as UTF-8 text when possible
  - raw body as base64 for non-UTF8 payloads

## Configuration

The app reads these env vars (matching `.env.example`):

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_DATABASE`

By default, the server listens on `0.0.0.0:3000`.

## Run

```bash
cargo run
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
