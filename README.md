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

## What's prepared but NOT yet done

- **Deploy (Fly.io)** — `Dockerfile` and `fly.toml` are here and untested
  against a real Fly.io account. Running `flyctl launch` / `flyctl deploy`
  is the next step, from a terminal that already has `flyctl` authenticated.
- **Load test (k6)** — `loadtest.js` is written (ramps to 300 VUs against
  `/work`, on purpose past the default rate-limit and shedder capacity) but
  has not been run against anything yet — not localhost, not deployed.
- **SLOs from measured data, deliberate failure, postmortem** — cannot exist
  honestly until the load test above has actually run and produced real
  numbers. Nothing here should be described as "load-tested" or "operating
  under defined SLOs" until this section is filled in with real k6 output.

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
