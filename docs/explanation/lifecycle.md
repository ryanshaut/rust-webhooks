# Delivery lifecycle and limitations

## Capturing and claiming are separate operations

Captured requests begin as `new` and active. Peek reads a pending record without
claiming it. Receive atomically claims the oldest available matching record and
sets it to `received` with a deadline.

| State | Active | How it is reached |
| --- | --- | --- |
| `new` | true | Capture succeeds |
| `received` | true | A consumer claims it |
| `success` | false | Consumer completes successfully |
| `failed` | false | Consumer completes with a failure |
| `expired` | false | A stale received claim is expired |

Check-in resets the deadline relative to database time and can store progress
text. It does not renew a claim that has already expired.

## Expiration is lazy and terminal

Peek, claim, complete, and check-in expire stale claims before doing their work.
There is no separate periodic job for claim expiration. Read-only operator
queries do not trigger expiration, so their counts can include received claims
whose deadlines have passed until a consumer operation runs.

An expired claim gets `substatus: "retry-ttl"` and becomes inactive. Despite that
name, the application does **not** automatically requeue or retry it. Likewise,
failure substatus strings are stored metadata, not retry instructions.

Disconnecting a consumer does not put delivered records back in the queue. A
consumer must complete or check in before the deadline; any retry or recovery
policy must be implemented externally.

## WebSocket delivery and replicas

WebSockets reuse database claiming but rely on process-local notifications.
This avoids periodic database polling for connected clients, at the cost of
requiring a producer and its subscriber to use the same server instance.

Reservations prevent overlapping HTTP/WebSocket receive on that instance only.
They do not coordinate across replicas. Inserts by another server or directly in
PostgreSQL do not wake a local subscriber. A send failure can occur after a
record is claimed; delivery is not an end-to-end exactly-once processing guarantee.

## Callbacks are a separate primitive

Callbacks provide a single-use JSON handoff: create a slot, let one payload win,
and poll until fulfillment. Equal retries are idempotent. Retention is bounded
and cleanup is periodic. Callback payloads are not consumer webhooks and do not
appear in operator webhook summaries or the webhook dashboards.

## Operational responsibilities

The application currently does not provide:

- Built-in authentication, authorization, TLS termination, or webhook signature verification.
- Automatic retry, dead-letter replay, or consumer recovery after a claim expires.
- Distributed WebSocket coordination or a shared rate-limit service.
- Payload-body redaction or automatic retention/deletion of captured webhooks.
- A production application deployment workflow.
- A measured 100% test-coverage guarantee.

Use trusted ingress, explicit data retention, and operational monitoring appropriate
to your deployment. See [running a container](../how-to/run-container.md) and
[monitoring](../how-to/monitor.md).
