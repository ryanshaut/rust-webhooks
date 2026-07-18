

import requests

base_url = "http://localhost:3000"


def send_webhook(endpoint, payload):
    url = f"{base_url}/api/webhooks/{endpoint}"
    response = requests.post(url, json=payload)
    return response.json()


def get_status(webhook_id):
    url = f"{base_url}/api/operator/webhooks/{webhook_id}/status"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def receive_webhook(topic):
    url = f"{base_url}/api/consumer/receive/{topic}"
    response = requests.post(url)
    response.raise_for_status()
    return response.json()

def close_webhook(webhook_id, ok=True, err=False):
    url = f"{base_url}/api/consumer/webhooks/{webhook_id}/complete"
    if not (ok or err) or (ok and err):
        raise ValueError("Either ok or err must be True, but not both")
    response = requests.post(url, json={'outcome': 'success' if ok else 'failed'})
    response.raise_for_status()
    return response.json()


def test(topic, is_success):
    w = send_webhook(topic, {"message": "Hello, World!"})
    print("Webhook sent:", w.get("id"))
    status = get_status(w.get("id"))
    print(f"Webhook status after submission: {status.get('id')}, status: {status.get('status')}, substatus: {status.get('substatus')}, active: {status.get('active')}")

    received = receive_webhook(topic)
    # print("Webhook received:", received)
    status = get_status(w.get("id"))
    print(f"Webhook status after receiving: {status.get('id')}, status: {status.get('status')}, substatus: {status.get('substatus')}, active: {status.get('active')}")

    
    closed = close_webhook(w.get("id"), ok=is_success, err=not is_success)
    # print("Webhook closed:", closed)
    status = get_status(w.get("id"))
    print(f"Webhook status after closing: {status.get('id')}, status: {status.get('status')}, substatus: {status.get('substatus')}, active: {status.get('active')}")

if __name__ == "__main__":

    import random

    i = random.randint(1, 1000)
    topic = f"test/foo/buzz_{i}"
    print("\n\nTesting with a successful webhook...\n\n")
    test(topic, is_success=True)

    i = random.randint(1, 1000)
    topic = f"test/foo/buzz_{i}"
    print("\n\nTesting with a failing webhook...\n\n")
    test(topic, is_success=False)