# Architecture

rust-webhooks is an Axum HTTP server with a SQLx PostgreSQL connection pool.
PostgreSQL is both the request store and the authority for claim state.

## Code organization

| Module | Responsibility |
| --- | --- |
| `src/main.rs` | Startup configuration, shared state/types, routing, cleanup task, listener |
| `src/webhooks.rs` | Capture, path dimensions, redaction, Base64 encoding |
| `src/callbacks.rs` | Callback create/poll/fulfill, rate limiting, expiration cleanup |
| `src/consumer.rs` | Peek/claim, WebSocket streaming, completion, check-in, claim expiry |
| `src/operator.rs` | Read-only counts and lifecycle inspection |
| `src/storage.rs` | Schema initialization, indexes, legacy dimension backfill |
| `src/tests.rs` | Unit, routing, WebSocket, and PostgreSQL integration tests |

The modules live in a single binary crate. They share application state and types
from the crate root rather than introducing a separate service layer or library API.

## Request flow

Generic webhook capture normalizes headers and decoded query values, redacts
configured sensitive keys, and inserts request data into `incoming_webhooks`.
Successful insertion notifies local WebSocket subscribers.

HTTP and WebSocket consumers share the same claim operation, using an atomic
PostgreSQL update with row locking and `SKIP LOCKED`. Multiple consumers can claim
different pending rows without delivering the same pending row twice.

Callback URLs are routed through a separate single-use slot mechanism backed by
`oneoff_hooks`. A UUID-only GET/POST suffix takes precedence over generic capture.

## Process-local coordination

Shared in-memory state tracks WebSocket reservations, change notifications, and
per-IP callback rate-limit windows. It is not a distributed broker or a persistent
coordination system. Database claim correctness and subscriber notification
boundaries are separate concerns; see [delivery limitations](lifecycle.md).

## Deployment boundaries

The API, PostgreSQL, and Grafana are separate processes. The development Compose
stack starts the database and monitoring tools, not the Rust API. Container
publishing produces the API image; it does not deploy a running application.

This documentation is a separate static MkDocs site deployed through GitHub Pages.
It neither embeds the Rust server nor requires a database.
