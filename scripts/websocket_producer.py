"""Send sample HTTP webhooks for the WebSocket consumer."""

import argparse
import sys
import time

import requests


def send_webhook(base_url, topic, payload):
    response = requests.post(
        f"{base_url.rstrip('/')}/api/webhooks/{topic}",
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:3000")
    parser.add_argument("--topic", default="test/foo/buzz", help="tenant/app/event")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1, help="Seconds between webhooks")
    args = parser.parse_args()
    if args.count <= 0 or args.interval < 0:
        parser.error("--count must be positive and --interval must be non-negative")

    for index in range(args.count):
        sent = send_webhook(
            args.base_url,
            args.topic,
            {"message": "Hello, World!", "sequence": index + 1},
        )
        print("Webhook sent:", sent["id"], flush=True)
        if index + 1 < args.count:
            time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except requests.RequestException as exc:
        print(f"Producer error: {exc}", file=sys.stderr)
        sys.exit(1)
