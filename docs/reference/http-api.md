# HTTP API reference

All routes are relative to the API server, not this documentation website.
There is no built-in authentication, authorization, or signature verification.
JSON endpoints use `Content-Type: application/json`.

## Capture

| Method | Path | Response |
| --- | --- | --- |
| Any | `/api/webhooks` | `202` with `id` and `received_at` |
| Any | `/api/webhooks/{*rest}` | `202` with `id` and `received_at`, except callback GET/POST routes below |

Requests store the method, path, headers, decoded query parameters, UTF-8 body
when valid, Base64 body, dimensions, and lifecycle fields.

The first three non-empty path segments after `/api/webhooks` become `tenant`,
`app`, and `event`. Additional segments remain in the stored path but do not add
dimensions. Partial paths are accepted, but consumers require both tenant and app.

Header values are arrays of strings. Header values that cannot be represented as
text are omitted. Query parameters are decoded into a string map, not an ordered
multi-value list. See [record format](storage.md#stored-webhook-record).

## Single-use callbacks

| Method | Path | Behavior |
| --- | --- | --- |
| POST | `/api/callbacks` | `201` with `id` and relative `callback_url` |
| GET | `/api/webhooks/{id}` | `202` with `{"status":"pending"}`; `200` with fulfilled JSON payload; `404` when unknown or expired |
| POST | `/api/webhooks/{id}` | Fulfill with any valid JSON value; `200` with `id` and `status: "fulfilled"` |

These GET/POST callback routes apply when the entire suffix is a valid UUID.
They take precedence over generic capture **even if no callback with that ID exists**.
Other methods on a single-UUID path use generic capture.

Example creation response:

```json
{
  "id": "a1d714a6-2d6a-4b46-8108-2e01bf366413",
  "callback_url": "/api/webhooks/a1d714a6-2d6a-4b46-8108-2e01bf366413"
}
```

The first fulfillment wins. Retrying an equal JSON payload returns `200`;
a different payload returns `409`. Invalid JSON returns `400`; an oversized body
returns `413`. Callback create, poll, and fulfill requests share the configured
per-IP rate limit (`429`). Expired callbacks return `404` even before physical cleanup.

Callbacks do not enter the consumer lifecycle or operator webhook counts.

## Consumer peek and receive

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/api/consumer/peek/{tenant}/{app}` | Peek across all events |
| GET | `/api/consumer/peek/{tenant}/{app}/{event}` | Peek one event |
| POST | `/api/consumer/receive/{tenant}/{app}` | Claim across all events |
| POST | `/api/consumer/receive/{tenant}/{app}/{event}` | Claim one event |

Peek and receive return `200` with a [stored webhook record](storage.md#stored-webhook-record),
or `204` with no body when no pending record matches. Candidates are ordered by
`received_at` ascending; claims use PostgreSQL row locking with `SKIP LOCKED`.

Each dimension accepts `*` as a whole-segment wildcard. Wildcards must be trailing:
`tenant/app/*`, `tenant/*/*`, and `*/*/*` are valid; `tenant/*/event` and
`*/app/event` return `400`. Omitting the event is equivalent to using `*`, so
`tenant/*` and `*/*` are also valid.

Receive accepts optional `ttl_seconds`, a positive integer; omission uses
`DEFAULT_RECEIVE_TTL_SECONDS`. Non-positive TTLs return `400` when TTL validation
is reached. An overlapping active WebSocket reservation returns `409`.

## Complete

`POST /api/consumer/webhooks/{id}/complete`

```json
{
  "outcome": "failed",
  "substatus": "retry-transient",
  "result": {"message": "upstream timeout"},
  "extra_properties": {"attempt": 1}
}
```

| Field | Contract |
| --- | --- |
| `outcome` | Required; `success` or `failed`, case-insensitive |
| `substatus` | Optional string; cleared for success |
| `result` | Optional JSON object (or null) |
| `extra_properties` | Optional JSON object (or null) |

Returns `200` with `id`, `status`, `active`, `substatus`, `result`, and
`extra_properties`. The update sets `active` false and clears `ttl_expires_at`.
Unsupported outcome or non-object metadata returns `400`. Missing, already
completed, or expired claims return `404`; completion is not idempotent.

## Check in

`POST /api/consumer/webhooks/{id}/check-in?ttl_seconds=300`

Optional JSON body:

```json
{
  "status_text": "Processing batch 2",
  "intermediate_status": "working"
}
```

Returns `200` with `id`, `status`, `ttl_expires_at`, `intermediate_status`,
and `status_text`. TTL is reset relative to the current database time, not added
to the previous deadline. Omitted or null status fields preserve existing values.

The claim must still be `received` and active; otherwise the response is `404`.
The TTL rules match receive.

## Operator

Operator routes are read-only: they do not expire stale claims as a side effect.

| Method | Path | Response |
| --- | --- | --- |
| GET | `/api/operator/status` | Aggregate counters |
| GET | `/api/operator/webhooks/{id}/status` | One webhook's lifecycle fields; `404` if absent |
| GET | `/api/operator/active-webhooks` | Active stream counts with optional filters |

The summary contains `pending_new`, `in_flight_received`, `completed_success`,
`completed_failed`, `expired_ttl`, and `total`, across `incoming_webhooks` only.

Per-webhook status contains `id`, `received_at`, dimensions, `status`, `active`,
`substatus`, `ttl_expires_at`, `intermediate_status`, `status_text`, `result`, and
`extra_properties`; it does not include the stored request payload.

Active-stream filters:

- `?tenant=shopify`
- `?tenant=shopify&app=orders`
- `?app=orders` without tenant returns `400`.

The result is an array of objects with `tenant`, `app`, `event`, `pending_new`,
`in_flight_received`, and `total_active`. Only fully dimensioned streams with at
least one pending `new` record are returned, sorted by tenant/app/event.
Received-only streams and partial-dimension captures are excluded.

## Errors

Database failures return `500`; most handlers include a JSON `error` message and
log the underlying failure to stderr. Axum handles malformed query/path/body
extraction and unsupported methods, so not every error has the same JSON shape.
Non-UUID IDs on UUID-typed consumer/operator routes return `400`.
