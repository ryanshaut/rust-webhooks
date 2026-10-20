# rust-webhooks

A Rust webhook catcher built with Axum, SQLx, and PostgreSQL. Capture HTTP
requests, claim and complete them through HTTP or WebSocket consumers, create
single-use JSON callbacks, and inspect lifecycle state through operator APIs
and Grafana.

## Documentation

The standalone documentation follows the Diataxis model:

| Section | Purpose |
| --- | --- |
| [Tutorials](docs/tutorials/first-webhook.md) | Learn with a complete webhook walkthrough |
| [How-to guides](docs/how-to/run-container.md) | Deploy containers, monitor, and test |
| [Reference](docs/reference/http-api.md) | API contracts, configuration, and storage formats |
| [Explanation](docs/explanation/architecture.md) | Architecture, lifecycle, and limitations |

**Website:** <https://ryanshaut.github.io/rust-webhooks/>
(available after GitHub Pages is enabled and the documentation workflow deploys).

## Quick start

With Rust, Docker Compose, and curl installed, run from the repository root:

```bash
docker compose -f db.docker-compose.yml up -d db
docker exec postgres pg_isready -U postgres
DB_USERNAME=postgres DB_PASSWORD= DB_HOST=localhost DB_PORT=5432 \
DB_DATABASE=rust-webhooks PORT=3001 cargo run
```

Wait for PostgreSQL to accept connections before starting the API. In another terminal:

```bash
curl -i -X POST http://localhost:3001/api/webhooks/demo/orders/created \
  -H 'Content-Type: application/json' \
  -d '{"order_id":123}'
```

Port 3001 avoids Grafana's port 3000. The schema is initialized automatically.
The API is unauthenticated and the development database uses trust authentication;
do not expose either to untrusted networks.

Published image: `ghcr.io/ryanshaut/rust-webhooks`.
See [container instructions](docs/how-to/run-container.md).

## Development

```bash
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

See [testing](docs/how-to/test.md) for PostgreSQL integration tests and k6 load tests.

## Environment variables

The server loads `.env` at startup when present. The table includes server settings
and variables used by the optional test, load-test, and development Compose setup.

| Variable | Used by | Default | Purpose |
| --- | --- | --- | --- |
| `DB_USERNAME` | Server | `postgres` | PostgreSQL username |
| `DB_PASSWORD` | Server | Empty | PostgreSQL password; omitted from the connection URL when empty |
| `DB_HOST` | Server | `localhost` | PostgreSQL host |
| `DB_PORT` | Server | `5432` | PostgreSQL port |
| `DB_DATABASE` | Server | `rust-webhooks` | PostgreSQL database name |
| `PORT` | Server | `3000` | API listening port |
| `DEFAULT_RECEIVE_TTL_SECONDS` | Server | `300` | Default webhook claim TTL when a request omits `ttl_seconds` |
| `CALLBACK_PENDING_TTL_SECONDS` | Server | `1209600` (14 days) | Lifetime of an unfulfilled callback |
| `CALLBACK_RETENTION_SECONDS` | Server | `259200` (72 hours) | Retention after a callback is fulfilled |
| `CALLBACK_MAX_BODY_BYTES` | Server | `1048576` (1 MiB) | Maximum callback fulfillment request body size |
| `CALLBACK_RATE_LIMIT_PER_MINUTE` | Server | `120` | Callback create, poll, and fulfillment requests per client IP per minute |
| `SENSITIVE_HEADERS` | Server | Built-in list | Comma-separated header names/patterns to redact; overrides the built-in list |
| `SENSITIVE_QUERY_KEYS` | Server | Built-in list | Comma-separated query parameter names/patterns to redact; overrides the built-in list |
| `TEST_DATABASE_URL` | PostgreSQL integration tests | None | Connection URL for the dedicated test database; not read by the server |
| `BASE_URL` | k6 load tests | `http://localhost:2999` in the k6 scripts; Make targets default to `http://172.17.0.1:3001` | API base URL |
| `VUS` | k6 load tests | `10` for webhook load tests, `30` for callback-user script; Make targets set their own values | Number of virtual users |
| `DURATION` | k6 load tests | `30s` in the scripts; `make k6-smoke` and `make k6-smoke-10k` use `60s` | Test duration |
| `RUN_ID` | Webhook k6 load test | `local` (`smoke` / `smoke-10k` in corresponding Make targets) | Identifier included in generated webhook query parameters |
| `POSTGRES_USER` | Development Compose database | `postgres` | PostgreSQL container's initial user |
| `POSTGRES_DB` | Development Compose database | `rust-webhooks` | PostgreSQL container's initial database |
| `POSTGRES_HOST_AUTH_METHOD` | Development Compose database | `trust` | PostgreSQL container authentication method; use safer authentication outside local development |

The server constructs its PostgreSQL connection URL from `DB_*`; it does not read
`DATABASE_URL`. Invalid or non-positive TTL and callback settings use their
defaults. An explicitly empty `SENSITIVE_HEADERS` or `SENSITIVE_QUERY_KEYS`
disables redaction for that category. The project does not configure any
Rust-specific environment variables.

## Sensitive keys redaction

Headers and query parameters with sensitive keys are redacted to `[REDACTED]`
before storage. Override the default lists with comma-separated, case-insensitive
environment variables:

```bash
SENSITIVE_HEADERS=authorization,x-api-key,custom-token
SENSITIVE_QUERY_KEYS=api_key,access_token,custom_secret
```

Overrides replace, rather than extend, the default lists. An explicitly empty
value disables that category of redaction.

- Exact match: `authorization`
- Suffix match: `*token`
- Contains match: `*secret*`

Redaction does not cover payload bodies or callback JSON payloads.

## Documentation development

```bash
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r docs/requirements.txt
.venv-docs/bin/python -m mkdocs serve --dev-addr 127.0.0.1:8000
```

Preview at <http://127.0.0.1:8000/rust-webhooks/>. Validate with
`.venv-docs/bin/python -m mkdocs build --strict`.

The [Documentation workflow](.github/workflows/docs.yml) validates documentation
pull requests and deploys `main` to GitHub Pages.
See [publishing documentation](docs/how-to/publish-docs.md) for one-time Pages setup.
