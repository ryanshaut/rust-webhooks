import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:2999';
const VUS = Number(__ENV.VUS || 30);
const DURATION = __ENV.DURATION || '30s';

// emulates the callback workflow of the app

export const options = {
  vus: VUS,
  duration: DURATION,
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

function new_callback() {
  const method = 'POST';
  const url = `${BASE_URL}/api/callbacks`;

  const headers = {
    'Content-Type': 'application/json',
    'X-Webhook-Client': 'k6',
    'X-Webhook-App': 'callback-demo',
  };

  const callback_res = http.request(method, url, null, { headers });


  return callback_res;
}

function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function respond_to_callback(callback, data) {
  const method = 'POST';

  const callback_url = callback.json('callback_url');
  const url = `${BASE_URL}${callback_url}`;

  const headers = {
    'Content-Type': 'application/json',
    'X-Webhook-Client': 'k6',
    'X-Webhook-App': 'callback-demo',
  };

  const res = http.request(method, url, JSON.stringify(data), { headers });
  // console.log('Responding to callback:', url, 'with data:', JSON.stringify(data));
  // console.log('Response status:', res.status, 'Response body:', res.body);
  if (res.status === 429) {
    console.log('Rate limited when responding to callback:', url);
  }
  check(res, {
    'callback responded': (r) => r.status < 300,
  });
  return res;
}

function sleep_for_time(seconds) {
  sleep(seconds);
}

export default function () {
  const callback = new_callback();
  check(callback, {
    'callback created': (res) => res.status < 300,
  });

  sleep_for_time(random(1, 3));
  respond_to_callback(callback, { message: 'Test callback' });
  return true;
}
