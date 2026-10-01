// sync-stall.js — k6 experiment for the directory-sync stall.
//
// Three phases, one story:
//   1. baseline (t=0-20s): healthy load, queue drains, sync accepted.
//   2. fault (t=20-50s): scale the sync pool to 1 replica mid-run —
//      the under-provisioned directory-sync incident — and watch the
//      backlog grow until batches are dropped.
//   3. recovery (t=50s+): scale back up, watch the backlog drain,
//      assert recovery. The first 2s after rollback are a grace
//      window while in-flight requests settle.
//
// Run: k6 run -e BASE_URL=http://localhost:8080 experiments/sync-stall.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const FAULT_AT = 20; // seconds
const ROLLBACK_AT = 50;
const GRACE = 2;

export const options = {
  scenarios: {
    steady_sync: {
      executor: 'constant-vus',
      vus: Number(__ENV.K6_VUS || 30),
      duration: '75s',
    },
  },
  thresholds: {
    // The fault phase is *supposed* to drop batches; what we assert
    // is that drops stay zero outside it and that every batch is
    // answered promptly (no unbounded hang).
    'sync_dropped_total{phase:baseline}': ['count==0'],
    'sync_dropped_total{phase:recovery}': ['count==0'],
    'sync_dropped_total{phase:fault}': ['count>0'], // the fault must actually bite
    'http_req_failed': ['rate<0.01'],
    'http_req_duration': ['p(95)<500'],
  },
};

// A 429 from /sync is the system shedding loudly, not a transport
// failure, so it doesn't count toward http_req_failed.
http.setResponseCallback(http.expectedStatuses(200, 202, 429));

const dropped = new Counter('sync_dropped_total');
const queueDepth = new Gauge('sync_queue_depth');
const acceptedBatches = new Counter('sync_accepted_total');
const waitTrend = new Trend('sync_avg_wait_ms');

function phaseAt(elapsed) {
  if (elapsed < FAULT_AT) return 'baseline';
  if (elapsed < ROLLBACK_AT) return 'fault';
  if (elapsed < ROLLBACK_AT + GRACE) return 'grace';
  return 'recovery';
}

function status() {
  const r = http.get(`${BASE_URL}/sync/status`);
  if (r.status === 200) {
    const s = r.json();
    queueDepth.add(s.queued);
    waitTrend.add(s.avg_wait_ms);
  }
  return r;
}

function scale(n) {
  http.post(`${BASE_URL}/scale?replicas=${n}`);
}

export function setup() {
  // Steady state before the fault: 6 replicas, queue draining.
  scale(6);
  sleep(1);
  return { start: Date.now() };
}

// VU 1 is the experiment controller; every VU generates load.
let faultInjected = false;
let rolledBack = false;

export default function (data) {
  const elapsed = (Date.now() - data.start) / 1000;
  const phase = phaseAt(elapsed);

  if (__VU === 1) {
    if (phase === 'fault' && !faultInjected) {
      scale(1);
      faultInjected = true;
    }
    if (elapsed >= ROLLBACK_AT && !rolledBack) {
      scale(6);
      rolledBack = true;
    }
  }

  const r = http.post(`${BASE_URL}/sync`, null, { tags: { phase } });
  check(r, { 'sync accepted or dropped loudly': (res) => res.status === 202 || res.status === 429 });
  // Add 0 on success too, so every phase's counter has samples and
  // its threshold is evaluated rather than skipped.
  dropped.add(r.status === 429 ? 1 : 0, { phase });
  if (r.status === 202) acceptedBatches.add(1, { phase });

  if (__ITER % 10 === 0) status();
  sleep(0.2);
}

export function teardown() {
  // Always leave the pool at full strength: every experiment rolls back.
  scale(6);
}
