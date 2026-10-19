# Receive webhooks over WebSocket

In this tutorial you will run a consumer that receives webhooks as they arrive and
completes them through HTTP.

First start the database and API using [Your first webhook](first-webhook.md).
Keep that API running on port 3001. You also need Python 3.11 or newer and
[uv](https://docs.astral.sh/uv/).

## 1. Install the example dependencies

From the repository root:

```bash
uv sync
```

## 2. Start the consumer

In one terminal:

```bash
uv run python scripts/websocket_consumer.py \
  --base-url http://localhost:3001 --topic test/foo --count 5
```

The consumer subscribes to all events for tenant `test` and app `foo`.
Leave it running while you start the producer.

## 3. Send five requests

In another terminal:

```bash
uv run python scripts/websocket_producer.py \
  --base-url http://localhost:3001 --topic test/foo/buzz --count 5
```

The producer sends HTTP requests; the consumer prints each received record and
marks it successful over HTTP. Both examples exit after five records.

## 4. Try an exact-event subscription

Repeat the consumer command with `--topic test/foo/buzz`. It now receives only
event `buzz`, rather than all events for `test/foo`.

Omit `--count` to keep the consumer running until Ctrl+C. Use `--ttl-seconds` on
the consumer to set its claim TTL and `--interval` on the producer to space out requests.
HTTPS base URLs are converted to `wss://` by the consumer.

!!! note "One overlapping receiver per server instance"
    An overlapping consumer fails with `409 Conflict`. Closing a connection releases
    its topic reservation, not its already delivered claims. These examples do not
    retry or perform long-running processing/check-ins.

See the [WebSocket reference](../reference/websocket-api.md) for connection errors,
overlap rules, and replica limitations.
