# Your first webhook

In this tutorial you will capture a request, inspect it, claim it for processing,
and mark it successful.

You need Rust and Cargo, Docker with Compose, and `curl`. Run commands from a local
checkout of the repository.

## 1. Start PostgreSQL

```bash
docker compose -f db.docker-compose.yml up -d db
docker exec postgres pg_isready -U postgres
```

Wait until PostgreSQL reports that it is accepting connections. If port 5432 is
already occupied, stop your conflicting local service or adapt the Compose port
mapping and `DB_PORT`.

## 2. Start the API

In a separate terminal:

```bash
DB_USERNAME=postgres DB_PASSWORD= DB_HOST=localhost DB_PORT=5432 \
DB_DATABASE=rust-webhooks PORT=3001 cargo run
```

The application creates its schema on startup. Wait for the listening message.
Port 3001 leaves port 3000 available for Grafana.

!!! warning "Local development only"
    The bundled database trusts connections without a password. Do not expose
    this database or the unauthenticated API to an untrusted network.

## 3. Capture a request

```bash
curl -i -X POST 'http://localhost:3001/api/webhooks/tutorial/orders/created?source=demo' \
  -H 'Content-Type: application/json' \
  -d '{"order_id":123,"status":"paid"}'
```

Expect `202 Accepted` with an `id` and `received_at`. The path assigns the request
to tenant `tutorial`, app `orders`, and event `created`.

## 4. Inspect and claim it

Peek without changing delivery state:

```bash
curl -sS http://localhost:3001/api/consumer/peek/tutorial/orders/created
```

The response includes the stored payload and `status: "new"`. Now claim it:

```bash
curl -sS -X POST \
  'http://localhost:3001/api/consumer/receive/tutorial/orders/created?ttl_seconds=300'
```

Expect `status: "received"` with `ttl_expires_at`. Copy this response's `id` and
set it in the terminal:

```bash
WEBHOOK_ID=replace-with-the-received-id
```

## 5. Complete the webhook

```bash
curl -sS -X POST \
  "http://localhost:3001/api/consumer/webhooks/${WEBHOOK_ID}/complete" \
  -H 'Content-Type: application/json' \
  -d '{"outcome":"success","result":{"processed":true}}'
```

Expect `status: "success"` and `active: false`. Confirm the final state:

```bash
curl -sS "http://localhost:3001/api/operator/webhooks/${WEBHOOK_ID}/status"
```

Stop the API with Ctrl+C when finished. If you started the database just for this
tutorial, stop it with `docker compose -f db.docker-compose.yml stop db`.

## What next?

- Try [receiving over WebSocket](websocket.md).
- Look up [check-in and callback endpoints](../reference/http-api.md).
- Read why [claim expiry is terminal rather than a retry queue](../explanation/lifecycle.md).
