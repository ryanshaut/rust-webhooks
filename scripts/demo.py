"""
End-to-end demo of the rust-webhooks API.

Demonstrates the full lifecycle:
  1. Send a webhook
  2. Operator checks aggregate status
  3. Consumer peeks at the queue
  4. Consumer receives (claims) the webhook
  5. Consumer checks in with intermediate status updates
  6. Consumer completes successfully with a result payload
  7. Operator inspects the final webhook status

Then repeats with a failing outcome to show that path.
"""

import json
import random
import sys

import requests

BASE_URL = "http://localhost:3000"


# ─── helpers ──────────────────────────────────────────────────────────────────

def pretty(label, data):
    print(f"\n  [{label}]")
    print(json.dumps(data, indent=4))


def section(title):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print(f"{'═' * 60}")


# ─── API wrappers ──────────────────────────────────────────────────────────────

def send_webhook(tenant, app, event, payload):
    url = f"{BASE_URL}/api/webhooks/{tenant}/{app}/{event}"
    r = requests.post(url, json=payload)
    r.raise_for_status()
    return r.json()


def operator_status():
    r = requests.get(f"{BASE_URL}/api/operator/status")
    r.raise_for_status()
    return r.json()


def operator_active_webhooks(tenant=None, app=None):
    params = {}
    if tenant:
        params["tenant"] = tenant
    if app:
        params["app"] = app
    r = requests.get(f"{BASE_URL}/api/operator/active-webhooks", params=params)
    r.raise_for_status()
    return r.json()


def operator_webhook_status(webhook_id):
    r = requests.get(f"{BASE_URL}/api/operator/webhooks/{webhook_id}/status")
    r.raise_for_status()
    return r.json()


def peek_webhook(tenant, app, event):
    r = requests.get(f"{BASE_URL}/api/consumer/peek/{tenant}/{app}/{event}")
    if r.status_code == 204:
        return None
    r.raise_for_status()
    return r.json()


def receive_webhook(tenant, app, event, ttl_seconds=60):
    r = requests.post(
        f"{BASE_URL}/api/consumer/receive/{tenant}/{app}/{event}",
        params={"ttl_seconds": ttl_seconds},
    )
    if r.status_code == 204:
        return None
    r.raise_for_status()
    return r.json()


def check_in(webhook_id, intermediate_status=None, status_text=None, ttl_seconds=60):
    body = {}
    if intermediate_status is not None:
        body["intermediate_status"] = intermediate_status
    if status_text is not None:
        body["status_text"] = status_text
    r = requests.post(
        f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/check-in",
        params={"ttl_seconds": ttl_seconds},
        json=body if body else None,
    )
    r.raise_for_status()
    return r.json()


def complete_webhook(webhook_id, outcome, substatus=None, result=None, extra_properties=None):
    body = {"outcome": outcome}
    if substatus:
        body["substatus"] = substatus
    if result is not None:
        body["result"] = result
    if extra_properties is not None:
        body["extra_properties"] = extra_properties
    r = requests.post(
        f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
        json=body,
    )
    r.raise_for_status()
    return r.json()


# ─── demo scenarios ────────────────────────────────────────────────────────────

def run_scenario(tenant, app, event, succeed: bool):
    outcome_label = "SUCCESS" if succeed else "FAILURE"
    section(f"Scenario: {tenant}/{app}/{event}  →  {outcome_label}")

    # 1. Send a webhook
    print("\n1. Sending inbound webhook...")
    sent = send_webhook(tenant, app, event, {
        "order_id": "ORD-9981",
        "amount": 149.99,
        "currency": "USD",
    })
    pretty("accepted", sent)
    webhook_id = sent["id"]

    # 2. Operator aggregate status
    print("\n2. Operator aggregate status...")
    pretty("aggregate", operator_status())

    # 3. Operator active webhooks (scoped to this tenant/app)
    print("\n3. Operator active webhooks (filtered)...")
    pretty("active", operator_active_webhooks(tenant=tenant, app=app))

    # 4. Consumer peek (non-destructive)
    print("\n4. Consumer peek (non-destructive)...")
    peeked = peek_webhook(tenant, app, event)
    pretty("peeked", peeked)

    # 5. Consumer receive (claims the webhook)
    print("\n5. Consumer receive (claims webhook, TTL=60s)...")
    received = receive_webhook(tenant, app, event, ttl_seconds=60)
    pretty("received", received)

    # 6. Check in — first intermediate update
    print("\n6. Check-in #1: fetching upstream data...")
    ci1 = check_in(
        webhook_id,
        intermediate_status="fetching",
        status_text="Fetching order details from upstream API...",
        ttl_seconds=60,
    )
    pretty("check-in #1", ci1)
    pretty("operator status after check-in #1", operator_webhook_status(webhook_id))

    # 7. Check in — second intermediate update
    print("\n7. Check-in #2: processing...")
    ci2 = check_in(
        webhook_id,
        intermediate_status="processing",
        status_text="Applying fulfillment rules to order ORD-9981...",
        ttl_seconds=60,
    )
    pretty("check-in #2", ci2)
    pretty("operator status after check-in #2", operator_webhook_status(webhook_id))

    # 8. Complete
    if succeed:
        print("\n8. Completing with SUCCESS + result payload...")
        completed = complete_webhook(
            webhook_id,
            outcome="success",
            result={
                "fulfillment_id": "FUL-4421",
                "shipped_at": "2026-07-20T14:30:00Z",
                "tracking_number": "1Z999AA10123456784",
            },
            extra_properties={
                "processed_by": "fulfillment-worker-3",
                "duration_ms": 312,
            },
        )
    else:
        print("\n8. Completing with FAILURE + substatus...")
        completed = complete_webhook(
            webhook_id,
            outcome="failed",
            substatus="upstream-timeout",
            extra_properties={
                "error_code": "UPSTREAM_504",
                "retryable": True,
            },
        )
    pretty("completed", completed)

    # 9. Final operator webhook status
    print("\n9. Final operator webhook status...")
    final = operator_webhook_status(webhook_id)
    pretty("final status", final)

    print(f"\n  Done — webhook {webhook_id} ended with status='{final['status']}'")


# ─── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_id = random.randint(1000, 9999)

    try:
        run_scenario(
            tenant="acme",
            app="orders",
            event=f"payment-received-{run_id}",
            succeed=True,
        )

        run_scenario(
            tenant="acme",
            app="orders",
            event=f"payment-failed-{run_id}",
            succeed=False,
        )
    except requests.HTTPError as exc:
        print(f"\nHTTP error: {exc.response.status_code} {exc.response.text}", file=sys.stderr)
        sys.exit(1)
    except requests.ConnectionError:
        print(f"\nCould not connect to {BASE_URL} — is the server running?", file=sys.stderr)
        sys.exit(1)
