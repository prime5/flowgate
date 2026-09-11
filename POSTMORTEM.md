# Incident Postmortem: Rate Limiter Ineffective Under Load

**Date:** 2026-09-11
**Author:** Pramathesh Malshe
**Severity:** No customer impact (personal/learning deployment). Real design gap, would matter in production.

## Summary

During the first load test against the live deployment (k6, ramping 1→300 VUs
over 2m30s against `https://flowgate-pramathesh.fly.dev`), the per-client rate
limiter admitted **100%** of traffic. Zero of 303,761 requests received a 429
(rate limit exceeded), despite a configured limit of 5 req/s sustained / 20
burst per client, against a single test client sustaining ~2,025 req/s.

## Impact

None externally — this is not a production service with real customers. The
gap is real, though: if a single abusive client were the reason rate limiting
existed, that client's requests would be treated as if they came from many
independent clients, and the per-client cap would do nothing to stop it.

## Measured results (k6, 2026-09-11)

| Metric | Value |
|---|---|
| Total requests | 303,761 |
| Sustained rate | ~2,025 req/s |
| Succeeded (200) | 264,971 |
| Shed (503) | 33,322 (~11.0%) |
| Backend errors (5xx, simulated) | 5,468 (~1.8%) |
| Rate limited (429) | 0 |
| Latency avg | 61ms |
| Latency p90 | 88.6ms |
| Latency p95 | 102ms |
| Latency max | 1.33s |

## Timeline (Pacific time, approximate)

- **T+0:00** — k6 ramp starts against the live Fly.io deployment.
- **T+2:30** — test completes. Shedder and simulated-backend-error numbers
  land close to expected; the 429 count is 0.
- **Post-test** — reviewed `internal/ratelimit/middleware.go`'s default
  `KeyFunc` and `cmd/flowgate/main.go`'s wiring: no custom `KeyFunc` is
  supplied, so the limiter keys on `r.RemoteAddr`.

## Root cause

`r.RemoteAddr` is the address Go's `net/http` server sees on the accepted TCP
connection, not necessarily the original client. Fly.io terminates client
connections at its edge/proxy layer and forwards to the app over its internal
network, so the app process sees connections from Fly's proxy infrastructure,
and the `RemoteAddr` on those connections does not reliably identify the real
originating client. Keying the rate limiter on `RemoteAddr` in this topology
means requests from what is really one client can be spread across many
different keys — so the 5 req/s-per-client cap is never actually enforced
against real traffic.

This is a deployment-topology gap, not a defect in the token-bucket algorithm
itself — `internal/limiter`'s unit tests, including the concurrent-access race
test, confirm the limiter is correct in isolation.

## Detection

Found by reading the actual k6 output rather than assuming the test validated
every component — the 0-count on the 429 metric was the signal that the
limiter wasn't engaging as designed. Not something that would have been
caught without running a real load test against the real deployment.

## What worked

- **Load shedder** correctly bounded in-flight concurrency and returned 503s
  once the configured 50-slot cap was exceeded, protecting the process itself
  from being the overload's first casualty (~11% of traffic shed).
- **Circuit breaker** tracked the ~2% simulated backend error rate throughout
  the run without tripping open — correct, since independent random failures
  at that rate shouldn't trip a 5-consecutive-failure threshold; a sustained
  backend outage should, and would need a separate test to confirm.
- **Latency** held at p95 = 102ms even at ~2,025 req/s sustained — no
  meaningful queueing or resource exhaustion observed at the gateway layer.

## What didn't

- **Rate limiter**: 0% effective in this deployment topology (see root cause).

## Remediation (not yet applied)

- Derive the client key from the `Fly-Client-IP` header (set by Fly.io's proxy
  to the real client IP) when present, falling back to `RemoteAddr` for local,
  non-proxied runs.
- Re-run the same k6 scenario after the fix and confirm 429s appear once a
  single client exceeds 5 req/s sustained.

## Follow-up

- [ ] Fix `KeyFunc` to use `Fly-Client-IP` with a `RemoteAddr` fallback.
- [ ] Re-run `loadtest.js` against the fixed deployment and record results here.
- [ ] Add a unit test asserting the middleware's default key function prefers
      a forwarded-client-IP header when present.
