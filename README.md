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
- `internal/fault` — fault-injection primitives: latency, error, timeout,
  blackhole, CPU burn, capacity removal, composable with `Chain` and
  gated by a blast-radius sampler. Another Go service can import it and
  opt in with one line of middleware.
- `internal/exp` — the experiment framework: steady-state hypothesis,
  one fault, observation, mandatory rollback. Produces a verdict that
  records the blast radius, so a result is reproducible evidence rather
  than an anecdote.
- `internal/pool` — dynamically resizable in-process worker pool: goroutines
  reading a bounded channel, with `SetSize` adding or stopping them at
  runtime. Models the directory-sync stall — under-provision capacity
  against the arrival rate and the backlog grows until batches miss their
  window. In-process, not an orchestrator: the workers are goroutines, not
  pods. `deploy/k8s` runs the same shape against real pods.
- `internal/fanout` — models a Greenplum-style MPP query: scatter across
  shards, gather results, with straggler and key-skew faults. Shows why
  total latency is the max of the shards and one bad shard dominates —
  the reason bulkheads and hedged requests exist.
- `internal/metrics` — Prometheus text-exposition writer, no external
  dependency: `flowgate_requests_total{outcome=...}`, `flowgate_breaker_state`,
  `flowgate_in_flight_requests`.
- `internal/mcp` — Model Context Protocol server over JSON-RPC 2.0 on
  stdio, hand-rolled with no third-party dependencies, for the same
  reason `internal/metrics` hand-writes Prometheus exposition. Exposes
  the experiment framework as agent-callable tools with policy caps
  enforced server-side.
- `cmd/flowgate` — the HTTP server. Gateway path: `/work` (simulated
  backend, ~2% error rate, 5-20ms latency) behind the full
  shed → rate-limit → breaker chain, plus `/metrics` and `/healthz`.
  Sync model: `POST /sync` (202 accepted, 429 when the queue is full),
  `GET /sync/status`, `POST /scale?replicas=N`. MPP model: `GET /query`
  with straggler and skew parameters. Experiments:
  `POST /experiments/pod-kill` and `POST /experiments/backend-latency`,
  each one steady-state check, one fault, one rollback. Only `/work`
  goes through the middleware chain; the rest are lab surface and
  bypass the limiter, shedder and breaker. `/scale` and `/experiments/*`
  mutate live state, so they are registered only when `FLOWGATE_LAB=1`
  and return 404 otherwise: `/metrics` is what the SLOs below are
  measured from, and those numbers are only attributable if nothing
  outside the process can move them.
- `cmd/faultdemo` — runs `internal/fault` and narrates each step, so the
  library's behaviour is observable without reading the tests.
- `cmd/flowgate-mcp` — the MCP entry point: exposes the experiment
  framework as three tools (`run_experiment`, `get_verdict`,
  `list_experiments`) over stdio, so an agent can run fault injections
  and read verdicts. The policy limits an agent cannot exceed are set
  here and enforced in `internal/mcp`, not in any prompt.

Run it locally:

```
go build -o flowgate ./cmd/flowgate
PORT=8080 ./flowgate
curl localhost:8080/work
curl localhost:8080/metrics
```

The lab endpoints are off by default. To run the experiments locally —
`experiments/sync-stall.js` POSTs `/scale`, so without the flag the
fault never bites and its `count>0` threshold fails:

```
FLOWGATE_LAB=1 PORT=8080 ./flowgate
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

## Resilience experiments (`resilience-experiments` branch)

A fault-injection playground on top of the gateway, shaped by three
real systems from my background. Every experiment has a steady
state, one fault, a blast radius, and a rollback — see
`INTERVIEW.md` for the narration.

- **Directory sync as a resizable worker pool** (`internal/pool`,
  `POST /sync`, `GET /sync/status`, `POST /scale?replicas=N`).
  Workers are pods; scaling is `kubectl scale`. Under-provision it
  and the backlog grows until batches miss their window — the
  Cloud Identity Engine pod-capacity incident, reproducible on
  demand. Scaling back up drains it; `deploy/k8s/hpa.yaml`
  automates the fix.
- **MPP fan-out query** (`internal/fanout`, `GET /query`): scatter
  work across shards, gather results. Fault primitives are a
  straggler shard (`&straggler=3&straggler_ms=500`) and a hot
  partition (`&skew=0.9`) — query latency is the *max* of the
  shards, so one bad shard dominates.
- **Experiment runner** (`internal/exp`, `POST
  /experiments/pod-kill`): steady-state hypothesis, one injected
  fault, declared blast radius, guaranteed rollback, bounded
  recovery window, verdict as JSON. `experiments/sync-stall.js` is
  the 75-second k6 version (baseline → fault → recovery, with
  per-phase drop thresholds); `scripts/ephemeral-env.sh` runs
  everything on a throwaway kind cluster; `scripts/pod-kill.sh`
  deletes real pods with a blast-radius guard.
- New metrics: `flowgate_sync_queue_depth`, `flowgate_sync_replicas`.
- Known limits: pool workers are goroutines, not pods (the k8s
  manifests run real pods); backend latency is simulated, queue
  dynamics are real.

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
