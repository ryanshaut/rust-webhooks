# WebSocket API reference

## Upgrade routes

| Route | Subscription |
| --- | --- |
| `GET /api/consumer/ws/{tenant}/{app}` | All events for tenant/app |
| `GET /api/consumer/ws/{tenant}/{app}/{event}` | Exact event |

Use `ws://` or `wss://` and a WebSocket upgrade. Optional `ttl_seconds` follows the
HTTP receive rules; non-positive TTLs return `400` before upgrade.

## Messages and lifecycle

The server sends one JSON text message containing the
[stored record](storage.md#stored-webhook-record) per claimed webhook. Existing
backlog is delivered after connection; newly captured records trigger delivery
without database polling. No client receive command is required.

Each delivered webhook has already transitioned to `received` and has a TTL.
Use the [HTTP check-in and completion endpoints](http-api.md) for lifecycle updates;
the WebSocket does not accept application-level acknowledgments.

Ping/pong is supported. Frame and incoming message limits are 1024 bytes.
Sends have a 10-second timeout; failed or slow sends terminate the connection.
On database receive failure the server attempts to send:

```json
{"error":"failed to receive webhook"}
```

It then ends the connection.

## Topic reservations

Two topics overlap when they share tenant/app and either omits the event or both
specify the same event.

- An overlapping WebSocket subscription returns HTTP `409` before upgrade.
- An overlapping HTTP receive also returns `409`.
- Non-overlapping receives remain available.
- Peek, complete, check-in, and operator endpoints are not blocked.

A WebSocket registration waits for an already in-progress overlapping HTTP
receive operation before acquiring its reservation.

Disconnecting releases the reservation, not previously delivered claims.
Those records remain received until completion or expiry.

## Instance boundaries

Reservations and notifications exist only in one server process. Producers and
their WebSocket consumers must reach the same instance. Other replicas and
direct PostgreSQL inserts do not notify a subscriber. See
[replica limitations](../explanation/lifecycle.md#websocket-delivery-and-replicas).
