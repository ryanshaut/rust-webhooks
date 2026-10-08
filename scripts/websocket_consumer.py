"""Receive webhooks over WebSocket and complete them over HTTP."""

import argparse
import json
import sys
from urllib.parse import urlsplit, urlunsplit

import requests
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect


def websocket_url(base_url, topic, ttl_seconds):
    url = urlsplit(base_url.rstrip("/"))
    if url.scheme not in ("http", "https") or not url.netloc:
        raise ValueError("--base-url must be an HTTP or HTTPS URL")
    scheme = "wss" if url.scheme == "https" else "ws"
    return urlunsplit((
        scheme,
        url.netloc,
        f"{url.path}/api/consumer/ws/{topic}",
        f"ttl_seconds={ttl_seconds}",
        "",
    ))


def close_webhook(base_url, webhook_id):
    response = requests.post(
        f"{base_url.rstrip('/')}/api/consumer/webhooks/{webhook_id}/complete",
        json={"outcome": "success"},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:3000")
    parser.add_argument(
        "--topic", default="test/foo", help="tenant/app (all events) or tenant/app/event",
    )
    parser.add_argument("--ttl-seconds", type=int, default=300)
    parser.add_argument("--count", type=int, default=0, help="Stop after N webhooks; 0 listens forever")
    args = parser.parse_args()
    if args.ttl_seconds <= 0 or args.count < 0:
        parser.error("--ttl-seconds must be positive and --count must be non-negative")

    url = websocket_url(args.base_url, args.topic, args.ttl_seconds)
    with connect(url, open_timeout=10) as socket:
        print(f"Subscribed to {args.topic}", flush=True)
        for index, message in enumerate(socket, start=1):
            webhook = json.loads(message)
            if "error" in webhook:
                raise RuntimeError(webhook["error"])
            print("Webhook received:", json.dumps(webhook, indent=2), flush=True)
            completed = close_webhook(args.base_url, webhook["id"])
            print(f"Webhook completed: {completed['id']} ({completed['status']})", flush=True)
            if args.count and index >= args.count:
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except (requests.RequestException, WebSocketException, OSError, ValueError, RuntimeError) as exc:
        print(f"Consumer error: {exc}", file=sys.stderr)
        sys.exit(1)
