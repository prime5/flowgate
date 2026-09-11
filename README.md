# flowgate

A small reliability gateway: token-bucket rate limiting, a circuit breaker, and
load shedding in front of a backend handler, with a hand-rolled Prometheus
`/metrics` endpoint. Built to learn Go by building and running something real,
not just to pass a tutorial.

## What's implemented (code, tested, race-checked)

- `internal/limiter` — token-bucket rate limiter, one bucket per client key,
  concurrency-safe. Unit tests cover burst behavior, refill over time, the
  capacity clamp, and a concurrent-access race test.
- `internal/breaker` — three-state circuit breaker (closed → open → half-open),
  with tests for the trip threshold, cooldown, and both half-open outcomes.
- `internal/shedder` — bounded in-flight concurrency; rejects immediately
  rather than queueing once the bound is hit. Tested under concurrent load.
- `internal/ratelimit` — the middleware that chains shed → rate-limit →
  breaker → backend, in that order, with metrics on every branch.
- `internal/metrics` — Prometheus text-exposition writer, no external
  dependency: `flowgate_requests_total{outcome=...}`, `flowgate_breaker_state`,
  `flowgate_in_flight_requests`.
- `cmd/flowgate` — the HTTP server: `/work` (simulated backend, ~2% error
  rate, 5-20ms latency), `/metrics`, `/healthz`.

Run it locally:

```
go build -o flowgate ./cmd/flowgate
PORT=8080 ./flowgate
curl localhost:8080/work
curl localhost:8080/metrics
```

Run the tests (with the race detector — this project promises concurrency
safety, so the race detector isn't optional):

```
go test ./... -race
```

## Deployed and load-tested

- **Deploy (Fly.io)** — live at `https://flowgate-pramathesh.fly.dev`
  (`fly.toml`, 2 machines, region `sjc`).
- **Load test (k6)** — `loadtest.js` run 2026-09-11 against the live
  deployment: ramp to 300 VUs, 303,761 requests, ~2,025 req/s sustained.
  264,971 succeeded, 33,322 shed (503, ~11%), 5,468 backend errors (~1.8%,
  matches the intentional simulated rate), **0 rate-limited (429)** — see
  `POSTMORTEM.md` for why, and the SLOs below for what this run established.

## SLOs (defined from the 2026-09-11 k6 run, not from theory)

| SLO | Target | Measured | Status |
|---|---|---|---|
| p95 request latency under ~2,000 req/s sustained | ≤ 150ms | 102ms | Met |
| Gateway responds to every request (no timeout/reset) | 100% | 100% | Met |
| Backend success rate for admitted (non-shed) requests | ≥ 95% | 97.98% | Met |
| Load shedding engages before the gateway itself degrades | shed, don't queue, above 50 in-flight | ~11% shed, latency stayed flat | Met |
| Per-client rate limit enforced (5 req/s sustained) | 429s appear for over-limit clients | 0 429s in 303,761 requests | **Not met** — see `POSTMORTEM.md` |

## What's prepared but NOT yet done

- **Fix the rate limiter's client-key derivation** — it currently keys on
  `r.RemoteAddr`, which doesn't reliably identify the real client behind
  Fly.io's proxy layer. `POSTMORTEM.md` has the root cause and the fix
  (key on the `Fly-Client-IP` header, falling back to `RemoteAddr`), plus a
  re-run to confirm 429s show up once it's fixed.

## Design notes

- Order in the middleware is shed → rate-limit → breaker, not the reverse:
  shedding is the cheapest check and protects the gateway process itself
  from being the thing that falls over; rate limiting is per-client and
  should run before spending a breaker check; the breaker is closest to the
  backend because it's reacting to the backend's actual health.
- The shedder rejects immediately instead of queueing. A queue would trade
  visible 503s for invisible latency — worse for the caller, and it hides
  the saturation point this project exists to find.
- `/metrics` has no external dependency on purpose — the Prometheus text
  format is simple enough to hand-write, and doing so means every line on
  that endpoint is something this project actually implements.
