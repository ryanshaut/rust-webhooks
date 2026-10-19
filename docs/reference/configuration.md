# Configuration reference

The application loads `.env` at startup using `dotenvy`, then reads environment
variables. There is no checked-in `.env.example`.

## Database and server

| Variable | Default | Meaning |
| --- | --- | --- |
| `DB_USERNAME` | `postgres` | PostgreSQL user |
| `DB_PASSWORD` | Empty | PostgreSQL password; omitted from the connection URL when empty |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_DATABASE` | `rust-webhooks` | Database name |
| `PORT` | `3000` | API port; binds on `0.0.0.0` |

The application constructs a PostgreSQL URL from these fields; it does not read a
runtime `DATABASE_URL` variable. The connection pool uses at most 10 connections.

An absent or unparseable `PORT` uses 3000. A parsed value of `0` is accepted by the
listener and requests an OS-assigned port.

## Lifecycle and callbacks

| Variable | Default | Meaning |
| --- | --- | --- |
| `DEFAULT_RECEIVE_TTL_SECONDS` | `300` | Claim TTL when the request omits `ttl_seconds` |
| `CALLBACK_PENDING_TTL_SECONDS` | `1209600` (14 days) | Lifetime of an unfulfilled callback |
| `CALLBACK_RETENTION_SECONDS` | `259200` (72 hours) | Lifetime after callback fulfillment |
| `CALLBACK_MAX_BODY_BYTES` | `1048576` (1 MiB) | Callback fulfillment body limit |
| `CALLBACK_RATE_LIMIT_PER_MINUTE` | `120` | Combined callback create/poll/fulfill requests per client IP per minute |

These variables use their defaults if absent, invalid, out of range for their Rust
integer type, or non-positive. Callback cleanup runs every 60 seconds and removes
at most 1000 expired rows per pass.

The callback limit is in memory and per server instance. The client IP is the TCP
peer address, not `X-Forwarded-For`; clients behind the same reverse proxy share
that proxy's IP bucket. A limited request returns `429` with `Retry-After: 60`.

Axum's default body extractor limit (2 MiB) also applies. Increasing
`CALLBACK_MAX_BODY_BYTES` does not override it.

## Header and query settings

`SENSITIVE_HEADERS` and `SENSITIVE_QUERY_KEYS` configure header and query
redaction. The existing
[README section](https://github.com/ryanshaut/rust-webhooks#sensitive-keys-redaction)
documents their use.

## Development tools

`TEST_DATABASE_URL` is read only by opt-in PostgreSQL tests, not by the server.
See [testing](../how-to/test.md).

Grafana in the development Compose stack also uses host port 3000. Use
`PORT=3001` for the API when running both.
