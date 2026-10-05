// k6 load test for flowgate. Run against a deployed instance:
//   BASE_URL=https://flowgate-pramathesh.fly.dev k6 run loadtest.js
// or against localhost while testing the ramp shape:
//   BASE_URL=http://localhost:8080 k6 run loadtest.js
//
// This ramps virtual users up past the point where flowgate's own
// rate limiter and shedder should start rejecting traffic (429/503),
// on purpose — the goal is to see the ceiling, not to avoid it.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// How many distinct client keys the load is spread across.
//
// This matters more than it looks. Every VU on one machine shares a
// source address, so without a per-client header the whole ramp keys
// to one bucket and the run measures a single client being held to
// 5 req/s — not per-client limiting. The 2026-10-04 localhost run did
// exactly that: 15,496,439 429s against 757 200s, and 5 req/s over
// 150s is ~750 requests. The limiter was correct; the test was
// single-client.
//
// Default: one key per VU, which is what the per-client SLO claims.
// CLIENTS=1 collapses them into one bucket, for showing a single
// over-limit client being held to its rate.
const CLIENTS = Number(__ENV.CLIENTS || 0); // 0 = one key per VU

function clientIP() {
  const n = CLIENTS > 0 ? __VU % CLIENTS : __VU;
  return `10.0.${Math.floor(n / 256)}.${n % 256}`;
}

export const options = {
  scenarios: {
    ramp_to_saturation: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 20 },   // warm up, should mostly get 200s
        { duration: '30s', target: 100 },  // past the default per-client rate limit
        { duration: '60s', target: 300 },  // push into shedder territory
        { duration: '30s', target: 0 },    // ramp down
      ],
    },
  },
  thresholds: {
    // The guard against the defect POSTMORTEM.md diagnosed. With one
    // key per VU the limiter must give each client its own 5 req/s, so
    // served requests should run to tens of thousands. A near-zero
    // count means the keys collapsed into one bucket again, which is
    // precisely the regression that went unnoticed before.
    flowgate_200_total: ['count>10000'],
  },
};

// 429 and 503 are the system rejecting loudly — the behaviour under
// test — so they are expected statuses rather than transport failures.
// 500 is deliberately NOT in this set: /work's simulated backend
// returns one ~2% of the time, and that should keep showing up in
// http_req_failed rather than being hidden here.
http.setResponseCallback(http.expectedStatuses(200, 429, 503));

const served = new Counter('flowgate_200_total');
const rateLimited = new Counter('flowgate_429_total');
const shedOrOpen = new Counter('flowgate_503_total');
const backendErrors = new Counter('flowgate_5xx_total');

export default function () {
  const res = http.get(`${BASE_URL}/work`, {
    headers: { 'Fly-Client-IP': clientIP() },
  });

  check(res, {
    // 500 belongs in the allowed set: the simulated backend returns one
    // ~2% of the time by design, so excluding it made documented
    // behaviour read as a failed check.
    'status is 200, 429, 503 or a simulated 500': (r) =>
      [200, 429, 503, 500].includes(r.status),
  });

  if (res.status === 200) served.add(1);
  if (res.status === 429) rateLimited.add(1);
  if (res.status === 503) shedOrOpen.add(1);
  if (res.status >= 500 && res.status !== 503) backendErrors.add(1);
}

// After the run, k6's own summary gives http_req_duration p(50)/p(95)/p(99)
// and the throughput (iterations/sec) — those are the numbers Phase F's
// SLOs get defined from. Don't hand-copy numbers from memory into the
// postmortem; paste the actual `k6 run` summary output.
//
// Two cautions when reading it. Iterations/sec counts rejections, which
// do no backend work, so a ramp that mostly 429s reports a throughput
// figure that is the speed of saying no — not capacity. Use
// http_req_duration{expected_response:true} for latency that reached
// the backend. And 503 covers both load shedding and an open circuit
// breaker; scrape /metrics after the run to separate them
// (flowgate_requests_total{outcome="shed"} vs {outcome="breaker_open"}).
