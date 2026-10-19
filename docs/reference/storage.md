# Storage and record format

## Schema

Startup creates and updates the schema with `ensure_schema()`; there is no
separate migration command. It also backfills path dimensions for legacy rows.

| Table | Contents |
| --- | --- |
| `incoming_webhooks` | Captured request data and consumer lifecycle |
| `oneoff_hooks` | Callback state, JSON payload, creation/fulfillment timestamps, and expiration |

Consumer lookup and TTL indexes are created on `incoming_webhooks`; callback
expiration has an index on `oneoff_hooks`.

## Stored webhook record

HTTP peek/receive and WebSocket delivery use the same record format:

| Field | JSON type / meaning |
| --- | --- |
| `id` | UUID string |
| `received_at` | Timestamp string |
| `method`, `path` | Strings |
| `tenant`, `app`, `event` | String or null |
| `query_params` | Object of string values, with sensitive keys redacted |
| `headers` | Object of arrays of string values, with sensitive keys redacted |
| `body_text` | UTF-8 string, or null for invalid UTF-8 |
| `body_base64` | Base64 string for **every** body, including UTF-8 bodies |
| `status` | `new`, `received`, `success`, `failed`, or `expired` |
| `active` | Boolean |
| `substatus` | String or null |
| `ttl_expires_at` | Timestamp string or null |
| `intermediate_status`, `status_text` | String or null |
| `result`, `extra_properties` | JSON object or null |

Payload bodies are stored without redaction. Header/query redaction cannot be
reversed from the stored record.

## Callback row

`oneoff_hooks` stores `id`, `status` (`pending` or `fulfilled`), nullable JSON
`payload`, `created_at`, nullable `fulfilled_at`, and `expires_at`. Payloads may be
any JSON value, not only objects.

An unfulfilled callback uses the pending TTL. First fulfillment resets expiration
to the retention period. Expired rows are deleted in bounded background batches.

## Retention

There is no automatic deletion or retention policy for `incoming_webhooks`.
Completion and expiry change state but do not remove request data. Plan database
backups, capacity, and any data deletion policy outside the application.
