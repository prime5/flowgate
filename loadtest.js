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
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

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
};

const rateLimited = new Counter('flowgate_429_total');
const shed = new Counter('flowgate_503_total');
const backendErrors = new Counter('flowgate_5xx_total');

export default function () {
  const res = http.get(`${BASE_URL}/work`);

  check(res, {
    'status is 200, 429, or 503': (r) => [200, 429, 503].includes(r.status),
  });

  if (res.status === 429) rateLimited.add(1);
  if (res.status === 503) shed.add(1);
  if (res.status >= 500 && res.status !== 503) backendErrors.add(1);
}

// After the run, k6's own summary gives http_req_duration p(50)/p(95)/p(99)
// and the throughput (iterations/sec) — those are the numbers Phase F's
// SLOs get defined from. Don't hand-copy numbers from memory into the
// postmortem; paste the actual `k6 run` summary output.
