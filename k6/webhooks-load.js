import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const VUS = Number(__ENV.VUS || 10);
const DURATION = __ENV.DURATION || '30s';

const METHODS = ['POST', 'PUT', 'PATCH', 'DELETE'];
const APPS = ['shopify', 'stripe', 'slack', 'github', 'custom-app'];
const EVENTS = ['orders', 'invoices', 'messages', 'deployments', 'billing'];

export const options = {
  vus: VUS,
  duration: DURATION,
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function buildPath() {
  const app = randomItem(APPS);
  const event = randomItem(EVENTS);
  const tenant = `tenant-${randomInt(1, 25)}`;
  return `/api/webhooks/${app}/${event}/${tenant}`;
}

function buildPayload() {
  return JSON.stringify({
    event_id: `evt_${Date.now()}_${randomInt(1000, 9999)}`,
    emitted_at: new Date().toISOString(),
    source: randomItem(APPS),
    schema_version: randomInt(1, 5),
    data: {
      amount: randomInt(10, 10000),
      currency: randomItem(['USD', 'EUR', 'GBP']),
      success: Math.random() > 0.1,
      tags: ['load-test', randomItem(EVENTS)],
    },
  });
}

export default function () {
  const method = randomItem(METHODS);
  const path = buildPath();
  const qs = `?source=k6&run_id=${__ENV.RUN_ID || 'local'}&vu=${__VU}&iter=${__ITER}`;
  const url = `${BASE_URL}${path}${qs}`;

  const headers = {
    'Content-Type': 'application/json',
    'X-Webhook-Client': 'k6',
    'X-Webhook-App': randomItem(APPS),
  };

  const res = http.request(method, url, buildPayload(), { headers });

  check(res, {
    'status is 202': (r) => r.status === 202,
  });

  sleep(Math.random() * 0.4 + 0.1);
}
