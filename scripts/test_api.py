"""Black-box API tests for a running rust-webhooks instance.

Run with:
    BASE_URL=http://127.0.0.1:3000 python -m unittest scripts.test_api -v

The suite creates uniquely named webhook dimensions and does not delete data,
so it should be run against a disposable test database.
"""

import base64
import os
import time
import unittest
import uuid

import requests


BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:3000").rstrip("/")


class WebhookApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = requests.Session()
        cls.prefix = f"test-{uuid.uuid4().hex[:10]}"
        try:
            response = cls.session.get(f"{BASE_URL}/api/operator/status", timeout=3)
        except requests.RequestException as exc:
            raise unittest.SkipTest(f"API is not reachable at {BASE_URL}: {exc}")
        if response.status_code != 200:
            raise unittest.SkipTest(
                f"API health check returned HTTP {response.status_code}: {response.text}"
            )

    def setUp(self):
        self.tenant = f"{self.prefix}-{self._testMethodName}"
        self.app = "orders"
        self.event = "created"

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def webhook_url(self, tenant=None, app=None, event=None):
        dimensions = [value for value in (tenant, app, event) if value is not None]
        return f"{BASE_URL}/api/webhooks/" + "/".join(dimensions)

    def status_url(self, webhook_id):
        return f"{BASE_URL}/api/operator/webhooks/{webhook_id}/status"

    def peek_by_id_url(self, webhook_id, tenant=None, app=None, event=None):
        return (
            f"{BASE_URL}/api/consumer/peek/"
            f"{tenant or self.tenant}/{app or self.app}/{event or self.event}/{webhook_id}"
        )

    def send(self, tenant=None, app=None, event=None, **kwargs):
        return self.session.post(
            self.webhook_url(tenant or self.tenant, app or self.app, event or self.event),
            timeout=5,
            **kwargs,
        )

    def test_capture_persists_dimensions_body_and_redacts_sensitive_values(self):
        response = self.send(
            params={"source": "integration", "access_token": "must-not-persist"},
            headers={"Authorization": "Bearer secret", "X-Request-ID": "request-1"},
            data=b"{\"order_id\":42}",
        )
        self.assertEqual(response.status_code, 202)
        accepted = response.json()
        webhook_id = accepted["id"]
        self.assertIn("received_at", accepted)

        status = self.session.get(self.status_url(webhook_id), timeout=5)
        self.assertEqual(status.status_code, 200)
        record = status.json()
        self.assertEqual(record["tenant"], self.tenant)
        self.assertEqual(record["app"], self.app)
        self.assertEqual(record["event"], self.event)
        self.assertEqual(record["status"], "new")
        self.assertTrue(record["active"])
        self.assertEqual(record["body_text"], '{"order_id":42}')
        self.assertEqual(
            record["body_base64"], base64.b64encode(b'{"order_id":42}').decode()
        )
        self.assertEqual(record["query_params"]["source"], "integration")
        self.assertEqual(record["query_params"]["access_token"], "[REDACTED]")
        self.assertEqual(record["headers"]["authorization"], ["[REDACTED]"])
        self.assertEqual(record["headers"]["x-request-id"], ["request-1"])

    def test_peek_is_non_destructive_and_receive_claims_oldest_matching_webhook(self):
        first = self.send(data=b"first").json()["id"]
        second = self.send(data=b"second").json()["id"]

        peek = self.session.get(
            f"{BASE_URL}/api/consumer/peek/{self.tenant}/{self.app}/{self.event}",
            timeout=5,
        )
        self.assertEqual(peek.status_code, 200)
        self.assertEqual(peek.json()["id"], first)

        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 30},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)
        received = receive.json()
        self.assertEqual(received["id"], first)
        self.assertEqual(received["status"], "received")
        self.assertTrue(received["active"])
        self.assertIsNotNone(received["ttl_expires_at"])

        next_receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 30},
            timeout=5,
        )
        self.assertEqual(next_receive.status_code, 200)
        self.assertEqual(next_receive.json()["id"], second)

    def test_peek_by_id_returns_full_record_without_changing_lifecycle_state(self):
        webhook_id = self.send(data=b"peek-by-id").json()["id"]

        new_peek = self.session.get(self.peek_by_id_url(webhook_id), timeout=5)
        self.assertEqual(new_peek.status_code, 200)
        self.assertEqual(new_peek.json()["body_text"], "peek-by-id")
        self.assertEqual(new_peek.json()["status"], "new")
        self.assertTrue(new_peek.json()["active"])

        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 30},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)

        received_peek = self.session.get(self.peek_by_id_url(webhook_id), timeout=5)
        self.assertEqual(received_peek.status_code, 200)
        self.assertEqual(received_peek.json()["status"], "received")
        self.assertTrue(received_peek.json()["active"])

        complete = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "success"},
            timeout=5,
        )
        self.assertEqual(complete.status_code, 200)

        completed_peek = self.session.get(self.peek_by_id_url(webhook_id), timeout=5)
        self.assertEqual(completed_peek.status_code, 200)
        self.assertEqual(completed_peek.json()["status"], "success")
        self.assertFalse(completed_peek.json()["active"])

        wrong_topic = self.session.get(
            self.peek_by_id_url(webhook_id, event="wrong-event"), timeout=5
        )
        self.assertEqual(wrong_topic.status_code, 404)

    def test_active_streams_and_filters_report_pending_and_in_flight_counts(self):
        received_id = self.send(data=b"in-flight").json()["id"]
        self.send(data=b"pending")
        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 30},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)
        self.assertEqual(receive.json()["id"], received_id)

        active = self.session.get(
            f"{BASE_URL}/api/operator/active-webhooks",
            params={"tenant": self.tenant, "app": self.app},
            timeout=5,
        )
        self.assertEqual(active.status_code, 200)
        stream = next(item for item in active.json() if item["event"] == self.event)
        self.assertEqual(stream["pending_new"], 1)
        self.assertEqual(stream["in_flight_received"], 1)
        self.assertEqual(stream["total_active"], 2)

        invalid_filter = self.session.get(
            f"{BASE_URL}/api/operator/active-webhooks",
            params={"app": self.app},
            timeout=5,
        )
        self.assertEqual(invalid_filter.status_code, 400)

    def test_check_in_updates_progress_and_success_completion_stores_objects(self):
        webhook_id = self.send(data=b"complete-me").json()["id"]
        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 10},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)

        check_in = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/check-in",
            params={"ttl_seconds": 30},
            json={"intermediate_status": "processing", "status_text": "Working"},
            timeout=5,
        )
        self.assertEqual(check_in.status_code, 200)
        self.assertEqual(check_in.json()["intermediate_status"], "processing")
        self.assertEqual(check_in.json()["status_text"], "Working")

        complete = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={
                "outcome": "SUCCESS",
                "result": {"processed": True},
                "extra_properties": {"source": "test"},
            },
            timeout=5,
        )
        self.assertEqual(complete.status_code, 200)
        completed = complete.json()
        self.assertEqual(completed["status"], "success")
        self.assertFalse(completed["active"])
        self.assertIsNone(completed["substatus"])
        self.assertEqual(completed["result"], {"processed": True})
        self.assertEqual(completed["extra_properties"], {"source": "test"})

        duplicate = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "success"},
            timeout=5,
        )
        self.assertEqual(duplicate.status_code, 404)

    def test_failed_completion_keeps_substatus_and_validation_rejects_bad_payloads(self):
        webhook_id = self.send(data=b"failed").json()["id"]
        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 30},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)

        invalid_outcome = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "unknown"},
            timeout=5,
        )
        self.assertEqual(invalid_outcome.status_code, 400)

        invalid_result = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "failed", "result": ["not", "an", "object"]},
            timeout=5,
        )
        self.assertEqual(invalid_result.status_code, 400)

        complete = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "failed", "substatus": "retry-transient"},
            timeout=5,
        )
        self.assertEqual(complete.status_code, 200)
        self.assertEqual(complete.json()["status"], "failed")
        self.assertEqual(complete.json()["substatus"], "retry-transient")

    def test_non_positive_ttl_is_rejected(self):
        self.send(data=b"ttl-validation")
        response = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 0},
            timeout=5,
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("greater than 0", response.json()["error"])

    def test_expired_claim_becomes_inactive_and_is_not_completable(self):
        webhook_id = self.send(data=b"expire-me").json()["id"]
        receive = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{self.tenant}/{self.app}/{self.event}",
            params={"ttl_seconds": 1},
            timeout=5,
        )
        self.assertEqual(receive.status_code, 200)

        time.sleep(1.2)
        trigger_expiry = self.session.get(
            f"{BASE_URL}/api/consumer/peek/{self.tenant}/{self.app}/{self.event}",
            timeout=5,
        )
        self.assertIn(trigger_expiry.status_code, (200, 204))

        status = self.session.get(self.status_url(webhook_id), timeout=5)
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["status"], "expired")
        self.assertFalse(status.json()["active"])
        self.assertEqual(status.json()["substatus"], "retry-ttl")

        expired_peek = self.session.get(self.peek_by_id_url(webhook_id), timeout=5)
        self.assertEqual(expired_peek.status_code, 200)
        self.assertEqual(expired_peek.json()["status"], "expired")
        self.assertFalse(expired_peek.json()["active"])

        complete = self.session.post(
            f"{BASE_URL}/api/consumer/webhooks/{webhook_id}/complete",
            json={"outcome": "success"},
            timeout=5,
        )
        self.assertEqual(complete.status_code, 404)

    def test_empty_queue_returns_no_content_and_unknown_status_is_not_found(self):
        tenant = f"{self.prefix}-empty"
        response = self.session.get(
            f"{BASE_URL}/api/consumer/peek/{tenant}/app/event", timeout=5
        )
        self.assertEqual(response.status_code, 204)
        response = self.session.post(
            f"{BASE_URL}/api/consumer/receive/{tenant}/app/event", timeout=5
        )
        self.assertEqual(response.status_code, 204)
        response = self.session.get(self.status_url(str(uuid.uuid4())), timeout=5)
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
