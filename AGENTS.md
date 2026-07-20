# rust-webhooks — Agent Instructions

Rust webhook catcher API using Axum + SQLx + PostgreSQL. Receives any HTTP request on `/api/webhooks/**`, stores it in Postgres, and exposes consumer/operator APIs for retrieval and lifecycle management.

## Build & Test

```bash
cargo build --release          # production build
cargo run                      # dev mode (debug build)
cargo test                     # run all tests
cargo fmt --all -- --check     # check formatting (CI gate)
cargo clippy --all-targets --all-features -- -D warnings  # lint (CI gate)
```

Use `make debug`, `make run`, `make test` as shortcuts (see [Makefile](Makefile)).

## Local Dev Setup

Start Postgres with Docker Compose:
```bash
docker compose -f db.docker-compose.yml up -d
```

Copy and configure env vars:
```bash
# Required env vars (no .env.example exists — set directly or via .env file):
DB_USERNAME=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_DATABASE=webhooks
DEFAULT_RECEIVE_TTL_SECONDS=300   # optional, default 300
PORT=3000                          # optional, default 3000
SENSITIVE_HEADERS=authorization,x-api-key   # optional, comma-separated
SENSITIVE_QUERY_KEYS=api_key,access_token   # optional, comma-separated
```

`dotenvy` loads `.env` automatically at startup. Schema is created via `ensure_schema()` on startup — no separate migration step needed.

## Architecture

All application code lives in [`src/main.rs`](src/main.rs) (single-file architecture).

**Key structs:**
- `AppState` — shared state holding `PgPool`, `SensitiveKeysConfig` (x2), and `default_ttl_seconds`
- `StoredWebhookRecord` / `CompletionResponse` / `OperatorActiveWebhookStreamResponse` — DB row types via `sqlx::FromRow`

**Route groups:**
| Prefix | Purpose |
|--------|---------|
| `POST /api/webhooks/**` | Capture any inbound webhook |
| `GET/POST /api/consumer/**` | Consumer peek/receive/complete/check-in |
| `GET /api/operator/**` | Operator observability and status |

**Webhook dimensions** (`tenant`, `app`, `event`) are parsed from the URL path when present.

**Sensitive key redaction** runs on headers and query params before DB storage. Patterns: exact match, suffix (`*token`), contains (`*secret*`). Override via `SENSITIVE_HEADERS` / `SENSITIVE_QUERY_KEYS` env vars.

## Load Testing

Requires Docker (no local k6 install):
```bash
make k6          # 10 VUs for 30s
make k6-smoke    # 100 VUs for 60s
BASE_URL=http://127.0.0.1:3000 VUS=50 DURATION=60s make k6
```

## CI/CD

See [docs/cicd.md](docs/cicd.md) for workflow details, image tagging conventions, and branch protection settings.

- Branch strategy: `feature/*` → PR → squash merge to `main`
- Container images published to `ghcr.io/ryanshaut/rust-webhooks`
- CI runs: `fmt --check`, `clippy -D warnings`, `test`, `build --release`

## Grafana Dashboards

Pre-built dashboards in [`grafana/dashboards/`](grafana/dashboards/). Provisioned automatically via [`grafana/provisioning/`](grafana/provisioning/). Push updates with `python scripts/push_grafana.py`.
