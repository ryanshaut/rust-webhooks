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
