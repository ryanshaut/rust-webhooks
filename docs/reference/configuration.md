# Configuration reference

The application loads `.env` at startup using `dotenvy`, then reads environment
variables. There is no checked-in `.env.example`. The table includes variables
used by the server as well as optional tests, load tests, and the development
Compose database.

| Variable | Used by | Default | Meaning |
| --- | --- | --- | --- |
| `DB_USERNAME` | Server | `postgres` | PostgreSQL user |
| `DB_PASSWORD` | Server | Empty | PostgreSQL password; omitted from the connection URL when empty |
| `DB_HOST` | Server | `localhost` | PostgreSQL host |
| `DB_PORT` | Server | `5432` | PostgreSQL port |
| `DB_DATABASE` | Server | `rust-webhooks` | PostgreSQL database name |
| `PORT` | Server | `3000` | API port; binds on `0.0.0.0` |
| `DEFAULT_RECEIVE_TTL_SECONDS` | Server | `300` | Claim TTL when a request omits `ttl_seconds` |
| `CALLBACK_PENDING_TTL_SECONDS` | Server | `1209600` (14 days) | Lifetime of an unfulfilled callback |
| `CALLBACK_RETENTION_SECONDS` | Server | `259200` (72 hours) | Lifetime after callback fulfillment |
| `CALLBACK_MAX_BODY_BYTES` | Server | `1048576` (1 MiB) | Callback fulfillment body limit |
| `CALLBACK_RATE_LIMIT_PER_MINUTE` | Server | `120` | Combined callback create/poll/fulfill requests per client IP per minute |
| `SENSITIVE_HEADERS` | Server | Built-in list | Comma-separated header names/patterns to redact; overrides the built-in list |
| `SENSITIVE_QUERY_KEYS` | Server | Built-in list | Comma-separated query parameter names/patterns to redact; overrides the built-in list |
| `TEST_DATABASE_URL` | PostgreSQL integration tests | None | Connection URL for a dedicated test database; not read by the server |
| `BASE_URL` | k6 load tests | `http://localhost:2999` in scripts; Make targets default to `http://172.17.0.1:3001` | API base URL |
| `VUS` | k6 load tests | Scripts: `10` for webhook load test, `30` for callback-user script; Make targets: `10` (`k6`), `100` (`k6-smoke`), `10000` (`k6-smoke-10k`) | Number of virtual users |
| `DURATION` | k6 load tests | Scripts and `make k6`: `30s`; smoke Make targets: `60s` | Test duration |
| `RUN_ID` | Webhook k6 load test | `local` in script and `make k6`; `smoke` / `smoke-10k` in corresponding Make targets | Identifier included in generated webhook query parameters |
| `POSTGRES_USER` | Development Compose database | `postgres` | PostgreSQL container's initial user |
| `POSTGRES_DB` | Development Compose database | `rust-webhooks` | PostgreSQL container's initial database |
| `POSTGRES_HOST_AUTH_METHOD` | Development Compose database | `trust` | PostgreSQL container authentication method; use safer authentication outside local development |

The application constructs a PostgreSQL URL from `DB_*`; it does not read a
runtime `DATABASE_URL` variable. The connection pool uses at most 10 connections.
The project does not configure any Rust-specific environment variables.

An absent or unparseable `PORT` uses 3000. A parsed value of `0` is accepted by the
listener and requests an OS-assigned port. Invalid or non-positive TTL and callback
settings use their documented defaults.

The sensitive-key settings accept comma-separated, case-insensitive values.
Patterns support exact matches, suffix matches such as `*token`, and contains
matches such as `*secret*`. A supplied value replaces, rather than extends, the
built-in list; an explicitly empty value disables redaction for that category.
Redaction runs on headers and query parameters before database storage, not on
payload bodies or callback JSON payloads.

Callback cleanup runs every 60 seconds and removes at most 1000 expired rows per
pass. The callback limit is in memory and per server instance. Its client IP is
the TCP peer address, not `X-Forwarded-For`; clients behind the same reverse proxy
share that proxy's IP bucket. A limited request returns `429` with
`Retry-After: 60`.

Axum's default body extractor limit (2 MiB) also applies. Increasing
`CALLBACK_MAX_BODY_BYTES` does not override it.

`TEST_DATABASE_URL` is used only by opt-in PostgreSQL tests, which create schema
and modify data. Use a dedicated disposable database; see
[testing](../how-to/test.md).

Grafana in the development Compose stack also uses host port 3000. Use
`PORT=3001` for the API when running both.
